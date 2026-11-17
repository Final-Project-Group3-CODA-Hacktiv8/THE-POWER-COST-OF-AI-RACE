# Final Project: AI Data Center Infrastructure & Global Energy Footprint

Proyek kolaboratif untuk menganalisa korelasi antara pertumbuhan pusat data AI global (Epoch AI) dan dampaknya terhadap kapasitas jaringan listrik serta emisi karbon regional (Ember Electricity Data).

---

## Team & Roles

* **Data Engineer (Zhafran Septama):** B Membangun pipeline ETL, Otomatisasi (Apache Airflow), Data cleaning dan Load data ke storage
* **Database & Documentation (Aisyah Sabrina Amalia):** DMerancang skema database, Manajemen database, Validasi kualitas data, serta Menyusun dokumentasi arsitektur sitem
* **Analyst & Dashboard (Aldi Zulkarnaen):**  Melakukan Exploratory Data Analysis (EDA), Membuat business insight dan rekomendasi, serta Merancang dashboard visualisasi interaktif

---

## Datasets

1. [Epoch AI Data Centers](https://epoch.ai/data/ai-data-centers)
2. [Ember Yearly Electricity Data](https://ember-energy.org/data/yearly-electricity-data/#explore-this-data)

---

## Problem Statement
Pembangunan data center AI di dunia tumbuh sangat cepat, sehingga kebutuhan listrik dan emisi karbonnya ikut melonjak. Kami membantu pembuat kebijakan energi, pengelola data center, dan investor menentukan di mana dan dengan standar efisiensi seperti apa data center AI berikutnya sebaiknya dibangun, dengan menganalisis data data center AI dunia dan data listrik di negara tempat data center itu berada, agar emisi dari data center AI baru bisa turun sekitar 20%, dari rata-rata saat ini ke tingkat data center paling ramah lingkungan yang sudah ada.

---

## Objective

Menganalisis seberapa besar, seberapa efisien, dan di mana data center AI dunia dibangun per 2026 menggunakan data Epoch AI dan Ember, untuk mengukur kapasitas listrik (MW), efisiensi energi (PUE), kebersihan sumber listrik (gCO2e/kWh), emisi tahunan, dan kapasitas komputasi per MW, sehingga pembangunan data center AI berikutnya bisa diarahkan ke lokasi dan standar yang lebih hemat energi dan rendah emisi.

---

## Key Questions

1. Bagaimana tren pertumbuhan kapasitas listrik (MW) dan kapasitas komputasi (H100e) data center AI dunia sejak 2021, dan perusahaan mana yang paling besar kontribusinya?
2. Seberapa besar konsumsi listrik dan estimasi emisi karbon data center AI dunia setiap tahun sejak 2021, dan bagaimana trennya?
3. Seberapa efisien data center AI, dilihat dari PUE serta perbandingan kapasitas komputasi (H100e) dengan konsumsi listrik, emisi, dan biaya operasional, dan data center mana yang paling efisien?
4. Di wilayah mana beban listrik data center AI paling besar dibanding total pembangkitan listrik wilayah tersebut?
5. Negara atau wilayah mana yang paling dan paling tidak ideal untuk membangun data center AI, berdasarkan intensitas karbon listrik, beban terhadap pembangkitan listrik, efisiensi, dan biaya operasional?

---

## System Architecture & Pipeline

```text
[Raw Data Sources: Epoch & Ember]
               │
               ▼
[ETL Pipeline & Airflow DAGs] (Data Engineer)
               │
               ▼
[PostgreSQL Database / Dimensional Galaxy Schema] (Database)
               │
               ▼
[EDA & Data Modeling] ──► [Interactive Dashboard] (Analyst)
```

* **Extraction & Transformation (ETL):** Raw data is processed using Python/Pandas and scheduled automatically via Apache Airflow DAGs.
* **Data Modeling:** Cleaned data is loaded into a structured relational database using a Fact Constellation (Galaxy) Schema.
* **Analysis & Visualization:** Data is processed to uncover historical trends, anomalies, and environmental impact projections through analytical dashboards.

---

## Data Dictionary

### A. `dim_country`
*Tabel yang menyimpan informasi negara tempat data center berada serta informasi geografis negara tersebut.*

| Nama Kolom | Tipe Data | Key / Constraint | Deskripsi |
| :--- | :--- | :--- | :--- |
| `country_name` | TEXT | Primary Key | Nama negara sebagai identitas unik negara. |
| `iso3_code` | TEXT | - | Kode negara berdasarkan standar ISO 3166-1 alpha-3, terdiri dari 3 huruf. |
| `continent` | TEXT | - | Nama benua tempat negara berada. |
| `is_asean` | BOOLEAN | - | Menunjukkan apakah negara merupakan anggota ASEAN (`TRUE`) atau bukan (`FALSE`). |

---

### B. `dim_us_state`
*Tabel yang menyimpan informasi negara bagian di Amerika Serikat. Digunakan untuk mengidentifikasi lokasi data center di wilayah AS.*

| Nama Kolom | Tipe Data | Key / Constraint | Deskripsi |
| :--- | :--- | :--- | :--- |
| `state_code` | CHAR(2) | Primary Key | Kode unik untuk negara bagian AS. |
| `state_name` | TEXT | NOT NULL | Nama negara bagian di AS. |

---

### C. `dim_data_center`
*Tabel yang menyimpan informasi identitas, lokasi, kepemilikan, dan pihak yang berkaitan dengan data center.*

| Nama Kolom | Tipe Data | Key / Constraint | Deskripsi |
| :--- | :--- | :--- | :--- |
| `datacenter_name` | TEXT | Primary Key, NOT NULL | Nama Data Center. |
| `country_name` | TEXT | Foreign Key | Negara tempat data center berada (`dim_country`). |
| `state_code` | CHAR(2) | Foreign Key | Kode negara bagian AS, bernilai `NULL` jika di luar AS (`dim_us_state`). |
| `owner_name` | TEXT | - | Nama perusahaan pemilik data center. |
| `owner_confidence` | TEXT | - | Tingkat keyakinan/keandalan informasi kepemilikan data center. |
| `users` | TEXT | - | Informasi pengguna layanan data center. |
| `investor` | TEXT | - | Nama pihak yang berinvestasi dalam pengembangan data center. |

---

### D. `dim_date`
*Tabel yang menyimpan informasi waktu berdasarkan tahun, digunakan untuk mengelompokkan data ke dalam periode tertentu.*

| Nama Kolom | Tipe Data | Key / Constraint | Deskripsi |
| :--- | :--- | :--- | :--- |
| `year` | INT | Primary Key | Tahun pencatatan data. |
| `decade` | TEXT | NOT NULL | Periode dekade yang mencakup tahun tersebut. |

---

### E. `fact_infrastructure`
*Tabel yang menyimpan informasi mengenai infrastruktur dan kapasitas operasional data center.*

| Nama Kolom | Tipe Data | Key / Constraint | Deskripsi |
| :--- | :--- | :--- | :--- |
| `datacenter_name` | TEXT | Foreign Key, Primary Key | Nama data center (`dim_data_center`). |
| `record_date` | DATE | Primary Key | Tanggal pencatatan atau pembaruan informasi. |
| `construction_status` | TEXT | - | Status pembangunan data center. |
| `buildings_operational` | NUMERIC | - | Jumlah bangunan/fasilitas yang telah beroperasi. |
| `it_power_mw` | NUMERIC | - | Kapasitas daya listrik untuk peralatan IT (dalam Megawatt). |
| `total_power_mw` | NUMERIC | - | Total kapasitas daya listrik fasilitas secara keseluruhan (dalam Megawatt). |
| `pue` | NUMERIC | - | Power Usage Effectiveness (Rasio total energi terhadap energi IT). |
| `h100e` | NUMERIC | - | Estimasi kapasitas komputasi setara chip Nvidia H100. |
| `annual_operating_cost_usd` | NUMERIC | - | Estimasi biaya operasional per tahun dalam Dolar AS (USD). |

---

### F. `fact_grid_country`
*Tabel yang menyimpan informasi pembangkit listrik dan emisi karbon berdasarkan negara, tahun, serta jenis sumber energi.*

| Nama Kolom | Tipe Data | Key / Constraint | Deskripsi |
| :--- | :--- | :--- | :--- |
| `country_name` | TEXT | Foreign Key, Part of PK | Nama negara lokasi pencatatan kelistrikan (`dim_country`). |
| `year` | INT | Foreign Key, Part of PK | Tahun pencatatan data (`dim_date`). |
| `electricity_source` | TEXT | Part of PK | Jenis sumber energi pembangkit listrik (cth: Coal, Solar, Total). |
| `generation_twh` | NUMERIC | - | Jumlah energi listrik yang dihasilkan (dalam TWh). |
| `emissions_mtco2e` | NUMERIC | - | Jumlah emisi gas rumah kaca (dalam Juta Ton $CO_2e$). |
| `intensity_gco2e_kwh` | NUMERIC | - | Intensitas emisi karbon (dalam $gCO_2e/kWh$). |

---

### G. `fact_grid_state`
*Tabel yang menyimpan informasi pembangkit listrik dan emisi karbon berdasarkan negara bagian di AS.*

| Nama Kolom | Tipe Data | Key / Constraint | Deskripsi |
| :--- | :--- | :--- | :--- |
| `state_code` | CHAR(2) | Foreign Key, Part of PK | Kode negara bagian AS lokasi pencatatan (`dim_us_state`). |
| `year` | INT | Part of PK | Tahun pencatatan data. |
| `electricity_source` | TEXT | Part of PK | Jenis sumber energi pembangkit listrik. |
| `generation_twh` | NUMERIC | - | Jumlah energi listrik yang dihasilkan (dalam TWh). |
| `emissions_mtco2e` | NUMERIC | - | Jumlah emisi gas rumah kaca (dalam Juta Ton $CO_2e$). |
| `intensity_g_kwh` | NUMERIC | - | Intensitas emisi karbon dari pembangkit listrik (dalam $g/kWh$). |

---

## Prerequisites & Setup

* Python 3.10+
* PostgreSQL / pgAdmin 4
* Docker & Apache Airflow (untuk orkestrasi pipeline ETL)

---

## Project Structure

```text
ai-energy-footprint-project/
├── dags/                  # Apache Airflow DAGs untuk otomatisasi ETL
├── data/                  # File data mentah (raw) dan hasil olahan (processed)
├── docs/                  # Dokumentasi diagram arsitektur & skema ERD
├── notebooks/             # Jupyter Notebooks untuk EDA & eksperimen data
├── sql/                   # Skrip DDL untuk pembuatan skema database
├── src/                   # Kode sumber pipeline ETL (Python scripts)
├── .env.example           # Template konfigurasi environment
├── requirements.txt       # Daftar dependensi Python
└── README.md              # Dokumentasi utama proyek
```

---

## License

Proyek ini dikembangkan untuk kebutuhan *final project* akademik dan profesional. Silakan hubungi anggota tim jika Anda memiliki pertanyaan atau ingin berkolaborasi lebih lanjut.