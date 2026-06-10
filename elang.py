import asyncio
import random
import sys
import time
import aiohttp

# Daftar kata kunci acak untuk mengacaukan Query Cache MySQL
SEARCH_QUERIES = [
    "gold",
    "silver",
    "ring",
    "necklace",
    "diamond",
    "luxury",
    "sale",
    "gift",
]


class WP_LoadTester_V2:

    def __init__(self, target_url, concurrency, rate_limit, duration, timeout=5):
        self.target_url = (
            target_url if target_url.endswith("/") else f"{target_url}/"
        )
        self.concurrency = concurrency
        self.rate_limit = rate_limit  # Fitur Baru: Batas RPS Target
        self.duration = duration
        self.timeout = timeout

        # Statistik
        self.total_sent = 0
        self.success_count = 0
        self.fail_count = 0
        self.status_codes = {}
        self.errors = {}
        self.latencies = []

    async def send_request(self, session, sem):
        """Mengirimkan satu request berdasarkan tipe endpoint WordPress."""
        # Menghormati batas Concurrency menggunakan Semaphore
        async with sem:
            # 1. Rotasi Endpoint Secara Cerdas
            mode = random.choice(["frontend", "search", "ajax"])
            headers = {
                "User-Agent": "WP-StressTester-Optimized/2.0 (Dynamic Async Bot)",
                "Cache-Control": "no-cache, no-store, must-revalidate",
                "Pragma": "no-cache",
            }

            start_req = time.time()
            self.total_sent += 1

            try:
                if mode == "frontend":
                    # Menembak halaman utama
                    url = self.target_url
                    async with session.get(
                        url,
                        headers=headers,
                        timeout=aiohttp.ClientTimeout(total=self.timeout),
                    ) as resp:
                        await resp.read()
                        status = resp.status

                elif mode == "search":
                    # Menembak query acak untuk menghancurkan MySQL Cache
                    query = random.choice(SEARCH_QUERIES)
                    url = f"{self.target_url}?s={query}_{random.randint(1,1000)}"
                    async with session.get(
                        url,
                        headers=headers,
                        timeout=aiohttp.ClientTimeout(total=self.timeout),
                    ) as resp:
                        await resp.read()
                        status = resp.status

                elif mode == "ajax":
                    # Perbaikan: Mengirim POST valid ke admin-ajax agar diproses Backend
                    url = f"{self.target_url}wp-admin/admin-ajax.php"
                    data = {
                        "action": "heartbeat",
                        "screen_id": "front",
                    }  # Payload standar WP
                    async with session.post(
                        url,
                        headers=headers,
                        data=data,
                        timeout=aiohttp.ClientTimeout(total=self.timeout),
                    ) as resp:
                        await resp.read()
                        status = resp.status

                # Hitung Latensi jika lolos
                latency = time.time() - start_req
                self.latencies.append(latency)
                self.success_count += 1
                self.status_codes[status] = (
                    self.status_codes.get(status, 0) + 1
                )

            except Exception as e:
                self.fail_count += 1
                err_name = type(e).__name__
                self.errors[err_name] = self.errors.get(err_name, 0) + 1

    async def run(self):
        print("=====================================================")
        print("        ELANG ASYNC WP PERFORMANCE TESTER         ")
        print("=====================================================")
        print(f"Target URL   : {self.target_url}")
        print(f"Concurrency  : {self.concurrency} Max Workers")
        print(f"Target RPS   : {self.rate_limit if self.rate_limit > 0 else 'UNLIMITED'}")
        print(f"Durasi Uji   : {self.duration} detik")
        print("=====================================================\n")

        start_test_time = time.time()
        end_time = start_test_time + self.duration

        # Batasan konkurensi token lokal
        sem = asyncio.Semaphore(self.concurrency)
        connector = aiohttp.TCPConnector(limit=None, ttl_dns_cache=300)

        async with aiohttp.ClientSession(connector=connector) as session:
            tasks = []
            while time.time() < end_time:
                # Loop penyeimbang kecepatan (Rate Limiter)
                start_frame = time.time()

                # Tentukan berapa request yang harus dilepas dalam frame 1 detik ini
                req_to_send = (
                    self.rate_limit
                    if self.rate_limit > 0
                    else self.concurrency
                )

                for _ in range(req_to_send):
                    if time.time() >= end_time:
                        break
                    task = asyncio.create_task(
                        self.send_request(session, sem)
                    )
                    tasks.append(task)

                # Fitur Baru: Pengendali Kecepatan agar stabil seperti Go Engine
                elapsed_frame = time.time() - start_frame
                if self.rate_limit > 0 and elapsed_frame < 1.0:
                    await asyncio.sleep(1.0 - elapsed_frame)
                else:
                    await asyncio.sleep(0.01)  # Jeda mikro anti-deadlock

            # Tunggu sisa request yang masih berjalan untuk diselesaikan
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)

        total_time = time.time() - start_test_time
        self.print_report(total_time)

    def print_report(self, total_time):
        print("\n==================== HASIL AKHIR ====================")
        print(f"Total Waktu Berjalan : {total_time:.2f} detik")
        print(f"Total Requests Sent  : {self.total_sent}")
        print(f"Requests Sukses      : {self.success_count}")
        print(f"Requests Gagal       : {self.fail_count}")
        print(f"Rata-rata RPS Aktual : {self.total_sent / total_time:.2f} req/sec")

        if self.latencies:
            self.latencies.sort()
            print("\nAnalisis Latensi (Hanya Sukses):")
            print(f"  - Rata-rata (Avg)  : {sum(self.latencies)/len(self.latencies):.4f} detik")
            print(f"  - p50 (Median)     : {self.latencies[int(len(self.latencies)*0.5)]:.4f} detik")
            print(f"  - p95 (95% User)   : {self.latencies[int(len(self.latencies)*0.95)]:.4f} detik")

        print("\nDetail Status Code   :")
        for code, count in self.status_codes.items():
            print(f"  - Status {code} : {count} kali")

        if self.errors:
            print("\nRincian Error/Kegagalan :")
            for err_msg, count in self.errors.items():
                print(f"  - [ {count} kali ] {err_msg}")
        print("=====================================================")


def main():
    target = input("1. Masukkan URL WordPress Target: ").strip()
    concurrency = int(input("2. Masukkan Max Concurrency (contoh: 50): "))
    rate_limit = int(input("3. Masukkan Batas RPS [0 untuk tanpa batas]: "))
    duration = int(input("4. Masukkan Durasi dalam detik (contoh: 60): "))

    tester = WP_LoadTester_V2(target, concurrency, rate_limit, duration)
    asyncio.run(tester.run())


if __name__ == "__main__":
    main()
    
