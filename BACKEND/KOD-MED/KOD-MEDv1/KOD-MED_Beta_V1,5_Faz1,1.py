# Faz 1 Aşama 1 ICD 10 kodlarinin Islenmesi

import pandas as pd
import logging
import io
from google.colab import files
import re

# Loglama ayarlarını yapılandır
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Çıktı dosyasının adını belirle
OUTPUT_CSV_FILENAME = 'umai_icd_codes.csv'

def clean_icd_code(code):
    """
    ICD kodunu temizler ve 'A00' formatına standardize eder.
    """
    if pd.isna(code) or code == '':
        return None

    # Kodu metne çevir, boşlukları temizle ve büyük harf yap
    code = str(code).strip().upper()

    if not code:
        return None

    # Geçerli ICD-10 formatını (Harf + 2 Rakam) kontrol et
    if re.match(r'^[A-Z]\d{2}', code):
        return code

    return None

def clean_disease_name(name):
    """
    Hastalık adındaki fazla boşlukları temizler.
    """
    if pd.isna(name) or name == '':
        return None

    # Metni string'e çevir ve kenar boşluklarını al
    name = str(name).strip()

    if not name:
        return None

    # Metin içindeki çoklu boşlukları tek boşluğa indirge
    name = re.sub(r'\s+', ' ', name)

    return name

def process_all_icd10_codes_colab():
    """
    Colab'da yüklenen bir ICD-10 Excel dosyasını okur,
    verileri temizler ve sonucu CSV olarak indirir.
    """
    try:
        # 1. Adım: Dosyayı Colab ortamına yükleme
        logging.info("Lütfen işlenecek ICD-10 Excel dosyasını seçin...")
        uploaded = files.upload()

        if not uploaded:
            logging.warning("Dosya yüklenmedi, işlem iptal edildi.")
            return

        file_name = next(iter(uploaded))
        file_content = uploaded[file_name]
        logging.info(f"'{file_name}' dosyası yüklendi, işleniyor...")

        # 2. Adım: Dosyayı okuma (farklı başlık satırlarını deneyerek)
        df = None
        for header_row in [0, 1, 2]:
            try:
                df = pd.read_excel(io.BytesIO(file_content), dtype=str, header=header_row)
                df.columns = [str(col).strip() for col in df.columns]
                valid_columns = [col for col in df.columns if not str(col).startswith('Unnamed')]
                if len(valid_columns) >= 2:
                    logging.info(f"Başlıklar {header_row}. satırda bulundu.")
                    break
            except Exception as e:
                logging.warning(f"Header={header_row} ile okuma başarısız: {e}")
                continue

        if df is None or len(df.columns) < 2:
            logging.error("Excel dosyası okunamadı veya yeterli sütun bulunamadı.")
            return

        logging.info(f"Dosyadaki sütunlar: {list(df.columns)}")

        # Kod ve açıklama için kullanılacak sütunları otomatik tespit et
        valid_columns = [col for col in df.columns if not str(col).startswith('Unnamed')]
        code_column = valid_columns[0]
        desc_column = valid_columns[1]
        logging.info(f"Kod sütunu: '{code_column}', Açıklama sütunu: '{desc_column}'")
        logging.info(f"Toplam {len(df)} satır okundu.")

        # 3. Adım: Veri temizleme
        df[code_column] = df[code_column].fillna('')
        df[desc_column] = df[desc_column].fillna('')

        cleaned_data = []
        for index, row in df.iterrows():
            clean_code = clean_icd_code(row[code_column])
            clean_name = clean_disease_name(row[desc_column])

            # Sadece geçerli kod ve isimleri listeye ekle
            if clean_code and clean_name:
                cleaned_data.append({
                    'icd_code': clean_code,
                    'disease_name': clean_name
                })

        # 4. Adım: Temizlenmiş veriyi DataFrame'e çevir
        final_df = pd.DataFrame(cleaned_data)

        if final_df.empty:
            logging.warning("Temizleme sonrası geçerli veri kalmadı.")
            return

        # Tekrarlanan kodları kaldır
        initial_count = len(final_df)
        final_df = final_df.drop_duplicates(subset=['icd_code'], keep='first')
        final_count = len(final_df)
        if initial_count != final_count:
            logging.info(f"{initial_count - final_count} adet tekrarlanan kod kaldırıldı.")

        # Veriyi ICD koduna göre sırala
        final_df = final_df.sort_values('icd_code').reset_index(drop=True)
        logging.info(f"Toplam {len(final_df)} geçerli ICD kodu işlendi.")

        # 5. Adım: Kod dağılımını göster
        code_distribution = final_df['icd_code'].str[0].value_counts().sort_index()
        logging.info(f"ICD Kod Dağılımı (İlk Harfe Göre):\n{code_distribution}")

        # 6. Adım: CSV olarak kaydet ve indir
        final_df.to_csv(OUTPUT_CSV_FILENAME, index=False, encoding='utf-8')
        logging.info(f"İşlenmiş dosya '{OUTPUT_CSV_FILENAME}' oluşturuldu.")

        # Kontrol için ilk ve son birkaç satırı göster
        logging.info(f"İlk 5 satır:\n{final_df.head()}")
        logging.info(f"Son 5 satır:\n{final_df.tail()}")

        # Dosyayı indir
        files.download(OUTPUT_CSV_FILENAME)
        logging.info("Dosya indirme işlemi başlatıldı.")

    except Exception as e:
        # Genel hata yakalama
        logging.error(f"İşlem sırasında bir hata oluştu: {e}")
        import traceback
        traceback.print_exc()

# Fonksiyonu çalıştır
process_all_icd10_codes_colab()
