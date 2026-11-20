import os
import shutil
import kagglehub

DATASET_HANDLE = "zhafranseptama/the-power-cost-of-ai-race"
RAW_DIR = "/workspace/data/Raw"


EXPECTED_FILES = [
    "data_centers.csv",
    "data_center_timelines.csv",
    "release_generation_yearly_global.csv",
    "release_us_subnational_generation_yearly.csv",
]


def run_extract():
    os.makedirs(RAW_DIR, exist_ok=True)

    print(f"Mengunduh dataset terbaru dari Kaggle: {DATASET_HANDLE} ...")
    download_path = kagglehub.dataset_download(DATASET_HANDLE, force_download=True)
    print(f"Dataset berhasil diunduh ke cache: {download_path}")
    
    copied_files = []
    for root, _, files in os.walk(download_path):
        for file in files:
            if file.endswith(".csv"):
                src = os.path.join(root, file)
                dst = os.path.join(RAW_DIR, file)
                shutil.copy2(src, dst)
                copied_files.append(file)
                print(f"Tersalin ke Raw: {dst}")

    missing_files = [f for f in EXPECTED_FILES if not os.path.exists(os.path.join(RAW_DIR, f))]
    if missing_files:
        raise FileNotFoundError(
            f"Extract gagal! File berikut tidak ditemukan di dataset Kaggle: {missing_files}. "
            f"File yang tersedia di Kaggle: {copied_files}"
        )

    print("Proses Extract selesai! Semua 4 file mentah siap di /workspace/data/Raw.")


if __name__ == "__main__":
    run_extract()