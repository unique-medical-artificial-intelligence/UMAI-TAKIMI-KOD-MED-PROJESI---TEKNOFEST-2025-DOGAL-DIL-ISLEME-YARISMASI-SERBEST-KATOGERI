
# Traning icin gerekli makale ve icd kodlarinin oldugu son CSV dosyasinin olusturulmasini saglayan kod

import pandas as pd
import json
import os
import zipfile
from google.colab import files
import time

def process_zip_data():
    """
    ZIP dosyasını yükle, JSON dosyalarını bul ve işle
    """
    print("ZIP dosyasını seçin:")
    uploaded = files.upload()

    if not uploaded:
        print("Dosya yüklenmedi!")
        return

    # Yüklenen dosyanın adını al
    zip_filename = list(uploaded.keys())[0]
    print(f"Yüklenen dosya: {zip_filename}")

    # ZIP dosyasını aç ve içeriğini kontrol et
    with zipfile.ZipFile(zip_filename, 'r') as zip_ref:
        print("ZIP içeriği:")
        file_list = zip_ref.namelist()

        # JSON dosyalarını bul
        json_files = [f for f in file_list if f.endswith('.json')]
        print(f"Bulunan JSON dosyaları: {len(json_files)} adet")

        if not json_files:
            print("ZIP dosyasında JSON dosyası bulunamadı!")
            return

        # Verileri işle
        all_records = []
        processed_count = 0

        for json_file in json_files:
            try:
                # JSON dosyasını oku
                with zip_ref.open(json_file) as f:
                    data = json.load(f)

                # ICD kodu ve makaleler varsa işle
                icd_code = data.get('icd_code')
                articles = data.get('articles', [])

                if icd_code and articles:
                    for article in articles:
                        title = article.get('title', '').strip()
                        if title:  # Boş olmayan başlıklar
                            all_records.append({
                                'text': title,
                                'icd_code': icd_code
                            })

                processed_count += 1
                if processed_count % 10 == 0:
                    print(f"İşlenen dosya sayısı: {processed_count}/{len(json_files)}")

            except Exception as e:
                print(f"✗ {json_file} işlenirken hata: {e}")
                continue

    # Sonuçları kontrol et
    if not all_records:
        print("Hiç veri bulunamadı!")
        return

    print(f"\nToplam {len(all_records)} kayıt bulundu")

    # DataFrame oluştur
    df = pd.DataFrame(all_records)

    # Temizlik işlemleri
    initial_count = len(df)
    df = df.drop_duplicates(subset=['text'])  # Tekrarları kaldır
    df = df[df['text'].str.len() > 5]  # Çok kısa metinleri kaldır
    final_count = len(df)

    print(f"Temizlik sonrası: {final_count} kayıt ({initial_count - final_count} kayıt temizlendi)")

    # CSV olarak kaydet
    output_filename = 'umai_labeled_dataset.csv'

    try:
        # CSV dosyasını oluştur
        df.to_csv(output_filename, index=False, encoding='utf-8')
        print(f"✓ CSV dosyası oluşturuldu: {output_filename}")

        # Dosyanın var olduğunu kontrol et
        if os.path.exists(output_filename):
            file_size = os.path.getsize(output_filename)
            print(f"Dosya boyutu: {file_size} bytes")

            # Sonuçları göster
            print(f"\nICD kod dağılımı:")
            print(df['icd_code'].value_counts().head(10))

            print(f"\nÖrnek veriler:")
            print(df.head())

            # İndirme işlemi için kısa bir bekleme
            time.sleep(1)

            # Dosyayı indir
            print("Dosya indiriliyor...")
            files.download(output_filename)
            print(f"✓ {output_filename} dosyası başarıyla indirildi!")

        else:
            print("❌ CSV dosyası oluşturulamadı!")

    except Exception as e:
        print(f"❌ CSV oluşturma/indirme hatası: {e}")

        # Alternatif yöntem: Manuel indirme
        print("\nAlternatif yöntem deneniyor...")
        try:
            # Dosyayı tekrar oluştur
            df.to_csv(output_filename, index=False, encoding='utf-8')

            # Manuel indirme
            from IPython.display import HTML
            import base64

            # Dosyayı base64 olarak kodla
            with open(output_filename, 'rb') as f:
                b64_data = base64.b64encode(f.read()).decode()

            # İndirme linki oluştur
            html = f'''
            <a download="{output_filename}" href="data:text/csv;base64,{b64_data}"
               style="background-color: #4CAF50; color: white; padding: 10px 20px;
                      text-decoration: none; border-radius: 5px;">
                📥 {output_filename} dosyasını indir
            </a>
            '''

            print("Manuel indirme linki oluşturuldu:")
            return HTML(html)

        except Exception as e2:
            print(f"❌ Alternatif yöntem de başarısız: {e2}")

            # Son çare: Veriyi ekrana yazdır
            print("\nVeri başarıyla işlendi ama indirilemedi.")
            print("CSV içeriğinin ilk 20 satırı:")
            print(df.head(20).to_string())
            print(f"\nToplam {len(df)} satır işlendi.")
            print("Bu çıktıyı kopyalayıp bir CSV dosyasına yapıştırabilirsiniz.")

# Fonksiyonu çalıştır
process_zip_data()