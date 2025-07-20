
# Islenen Json ve ZIP dosyasini  inceleyen kod

import pandas as pd
import json
import os
import zipfile
from google.colab import files
import time

def find_and_process_filled_articles():
    """
    Dolu makale listesi olan JSON dosyalarını bul ve işle
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
        empty_articles_count = 0
        filled_articles_count = 0
        processed_count = 0
        filled_files = []

        for json_file in json_files:
            try:
                # JSON dosyasını oku
                with zip_ref.open(json_file) as f:
                    data = json.load(f)

                # ICD kodu ve makaleler varsa işle
                icd_code = data.get('icd_code')
                disease_name = data.get('disease_name')
                articles = data.get('articles', [])

                if len(articles) > 0:  # Dolu makale listesi varsa
                    filled_articles_count += 1
                    filled_files.append({
                        'file': json_file,
                        'icd_code': icd_code,
                        'disease_name': disease_name,
                        'article_count': len(articles)
                    })

                    # Makaleleri işle
                    if icd_code and articles:
                        for article in articles:
                            # Makale yapısını kontrol et
                            if isinstance(article, dict):
                                # Başlık alanlarını kontrol et
                                title = article.get('title') or article.get('name') or article.get('headline')
                                if title and title.strip():
                                    all_records.append({
                                        'text': title.strip(),
                                        'icd_code': icd_code
                                    })
                            elif isinstance(article, str):
                                # Doğrudan string ise
                                if article.strip():
                                    all_records.append({
                                        'text': article.strip(),
                                        'icd_code': icd_code
                                    })
                else:
                    empty_articles_count += 1

                processed_count += 1
                if processed_count % 100 == 0:
                    print(f"İşlenen dosya sayısı: {processed_count}/{len(json_files)}")

            except Exception as e:
                print(f"✗ {json_file} işlenirken hata: {e}")
                continue

    # Sonuçları kontrol et
    print(f"\n📊 İşlem Sonuçları:")
    print(f"• Toplam işlenen dosya: {processed_count}")
    print(f"• Boş makale listesi olan dosya: {empty_articles_count}")
    print(f"• Dolu makale listesi olan dosya: {filled_articles_count}")
    print(f"• Oluşturulan kayıt sayısı: {len(all_records)}")

    # Dolu dosyaları göster
    if filled_files:
        print(f"\n📁 Dolu makale listesi olan dosyalar:")
        for file_info in filled_files[:10]:  # İlk 10 dosya
            print(f"  • {file_info['file']}: {file_info['article_count']} makale")

    if not all_records:
        print("❌ Hiç makale verisi bulunamadı!")

        # Sadece hastalık isimlerini kullan
        print("\n🔄 Hastalık isimlerini kullanarak veri seti oluşturuluyor...")
        disease_records = []

        for json_file in json_files:
            try:
                with zip_ref.open(json_file) as f:
                    data = json.load(f)

                icd_code = data.get('icd_code')
                disease_name = data.get('disease_name')

                if icd_code and disease_name:
                    disease_records.append({
                        'text': disease_name.strip(),
                        'icd_code': icd_code
                    })
            except:
                continue

        if disease_records:
            df = pd.DataFrame(disease_records)
            df = df.drop_duplicates(subset=['text'])

            output_filename = 'disease_names_dataset.csv'
            df.to_csv(output_filename, index=False, encoding='utf-8')

            print(f"✅ Hastalık isimleri veri seti oluşturuldu: {len(df)} kayıt")
            print(df.head())

            files.download(output_filename)
            print(f"✅ {output_filename} dosyası indirildi!")

        return

    # DataFrame oluştur
    df = pd.DataFrame(all_records)

    # Temizlik işlemleri
    initial_count = len(df)
    df = df.drop_duplicates(subset=['text'])
    df = df[df['text'].str.len() > 5]
    final_count = len(df)

    print(f"\nTemizlik sonrası: {final_count} kayıt ({initial_count - final_count} kayıt temizlendi)")

    # CSV olarak kaydet
    output_filename = 'articles_dataset.csv'

    try:
        df.to_csv(output_filename, index=False, encoding='utf-8')
        print(f"✅ CSV dosyası oluşturuldu: {output_filename}")

        if os.path.exists(output_filename):
            file_size = os.path.getsize(output_filename)
            print(f"📁 Dosya boyutu: {file_size} bytes")

            print(f"\n📈 ICD kod dağılımı:")
            print(df['icd_code'].value_counts().head(10))

            print(f"\n📄 Örnek veriler:")
            print(df.head())

            files.download(output_filename)
            print(f"✅ {output_filename} dosyası başarıyla indirildi!")

    except Exception as e:
        print(f"❌ CSV oluşturma/indirme hatası: {e}")

# Fonksiyonu çalıştır
find_and_process_filled_articles()