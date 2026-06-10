import asyncio
import random
import sys
import time
import aiohttp

# --- KONFIGURASI TARGET WORDPRESS ---
# Pilihan endpoint untuk menguji titik lemah WordPress
ENDPOINTS = [
    "",  # Halaman Utama (Menguji Cache Front-end)
    "?s=jewelry",  # Fitur Pencarian (Memaksa MySQL bekerja keras)
    "wp-admin/admin-ajax.php",  # WP AJAX (Bypass cache, hantam CPU Backend)
]


class WP_LoadTester:

    def __init__(self, target_url, concurrency, duration, timeout=5):
        # Memastikan format URL benar
        self.target_url = (
            target_url if target_url.endswith("/") else f"{target_url}/"
        )
        self.concurrency = concurrency
        self.duration = duration
        self.timeout = timeout

        # Statistik
        self.total_sent = 0
        self.success_count = 0
        self.fail_count = 0
        self.status_codes = {}
        self.errors = {}
        self.latencies = []

    async def fetch(self, session, worker_id):
        """Worker individual yang menembak WordPress secara konstan."""
        headers = {
            "User-Agent": f"WP-LoadTester-Bot/3.0 (Worker {worker_id}; Parallel Test)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Cache-Control": "no-cache",  # Memaksa server tidak memberi cache usang
        }

        end_time = time.time() + self.duration

        while time.time() < end_time:
            # Rotasi endpoint secara acak untuk mensimulasikan perilaku user nyata
            chosen_endpoint = random.choice(ENDPOINTS)
            full_url = f"{self.target_url}{chosen_endpoint}"

            self.total_sent += 1
            start_req = time.time()

            try:
                # Menggunakan ClientTimeout untuk membatasi waktu tunggu
                async with session.get(
                    full_url,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=self.timeout),
                ) as response:

                    await response.read()  # Membaca body agar koneksi selesai utuh
                    latency = time.time() - start_req
                    self.latencies.append(latency)

                    self.success_count += 1
                    self.status_codes[response.status] = (
                        self.status_codes.get(response.status, 0) + 1
                    )

            except Exception as e:
                self.fail_count += 1
                err_name = type(e).__name__
                self.errors[err_name] = self.errors.get(err_name, 0) + 1

            # Jeda mikro mikro agar tidak terjadi deadlock di CPU lokal
            await asyncio.sleep(0.001)

    async def run(self) -> None:
        """Inisialisasi session pool dan menyalakan seluruh worker."""
        print("=====================================================")
        print("       ELANG ASYNC WP PERFORMANCE TESTER          ")
        print("=====================================================")
        print(f"Target URL   : {self.target_url}")
        print(f"Concurrency  : {self.concurrency} Async Workers")
        print(f"Durasi Uji   : {self.duration} detik")
        print(f"Timeout      : {self.timeout} detik")
        print("=====================================================\n")
        print("Mengonfigurasi jaringan lokal... Pengujian dimulai.")

        start_test_time = time.time()

        # Konektor tanpa batas limit lokal agar maksimal
        connector = aiohttp.TCPConnector(limit=None, ttl_dns_cache=300)

        async with aiohttp.ClientSession(connector=connector) as session:
            # Membuat banyak worker (coroutine) sesuai jumlah concurrency
            tasks = [
                self.fetch(session, i) for i in range(self.concurrency)
            ]
            await asyncio.gather(*tasks)

        total_time = time.time() - start_test_time
        self.print_report(total_time)

    def print_report(self, total_time):
        """Mencetak analisis statistik persentil akhir."""
        print("\n==================== HASIL AKHIR ====================")
        print(f"Total Waktu Berjalan : {total_time:.2f} detik")
        print(f"Total Requests Sent  : {self.total_sent}")
        print(f"Requests Sukses      : {self.success_count}")
        print(f"Requests Gagal       : {self.fail_count}")

        if total_time > 0:
            print(f"Rata-rata Kecepatan  : {self.total_sent / total_time:.2f} RPS")

        if self.latencies:
            self.latencies.sort()
            avg_dur = sum(self.latencies) / len(self.latencies)
            p50 = self.latencies[int(len(self.latencies) * 0.50)]
            p95 = (
                self.latencies[int(len(self.latencies) * 0.95)]
                if len(self.latencies) >= 20
                else p50
            )
            print("\nAnalisis Latensi (Hanya Sukses):")
            print(f"  - Rata-rata (Avg)  : {avg_dur:.4f} detik")
            print(f"  - p50 (Median)     : {p50:.4f} detik")
            print(f"  - p95 (95% User)   : {p95:.4f} detik")

        if self.status_codes:
            print("\nDetail Status Code   :")
            for code, count in self.status_codes.items():
                ket = "OK" if code == 200 else "Error/Bad Gateway"
                print(f"  - Status {code} ({ket}) : {count} kali")

        if self.errors:
            print("\nRincian Error Jaringan/Timeout :")
            for err_msg, count in self.errors.items():
                print(f"  - [ {count} kali ] {err_msg}")
        print("=====================================================")


def main():
    # Input parameter interaktif
    target = input("1. Masukkan URL WordPress Target (https://site.com): ").strip()
    if not target.startswith("http"):
        print("[ERROR] URL harus diawali dengan http:// atau https://")
        sys.exit(1)

    try:
        concurrency = int(input("2. Masukkan Concurrency/Workers (contoh: 50): "))
        duration = int(input("3. Masukkan Durasi Uji dalam detik (contoh: 30): "))
    except ValueError:
        print("[ERROR] Input harus berupa angka bulat!")
        sys.exit(1)

    # Menjalankan loop async Python
    tester = WP_LoadTester(target, concurrency, duration)
    asyncio.run(tester.run())


if __name__ == "__main__":
    main()

