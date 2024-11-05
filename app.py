import requests
import zipfile
import os
import csv
import datetime
import pandas as pd
pd.pandas.set_option('display.max_columns', None)
import warnings
warnings.filterwarnings('ignore')
import shutil

class DownloadData:
    def get_nse_bhavcopy(url, zip_path, new_csv_path):
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            with open(zip_path, 'wb') as file:
                file.write(response.content)
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                csv_filename = zip_ref.namelist()[0]
                zip_ref.extract(csv_filename)
            os.rename(csv_filename, new_csv_path)
            os.remove(zip_path)
            print("NSE Bhavcopy Downloaded")
        else:
            print("NSE Bhavcopy Dwonload Failed")

    def get_nse_deliverables(url, csv_path):
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            response.raise_for_status()
            filtered_content = "\n".join(response.text.splitlines()[3:])
            with open(csv_path, 'w') as file:
                file.write(filtered_content)
            print("NSE Deliverables Downloaded")
        else:
            print("NSE Deliverables Dwonload Failed")

    def get_bse_bhavcopy(url, csv_path):
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            with open(csv_path, 'wb') as file:
                file.write(response.content)
            print("BSE Bhavcopy Downloaded")
        else:
            print("BSE Bhavcopy Downlload Failed")

    def get_bse_deliverables(url, zip_path, txt_filename, csv_path):
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            with open(zip_path, 'wb') as f:
                f.write(response.content)
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extract(txt_filename)
            with open(txt_filename, 'r', encoding='utf-8') as txtfile, open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
                reader = csv.reader(txtfile, delimiter='|')
                writer = csv.writer(csvfile)
                for row in reader:
                    writer.writerow(row)
            os.remove(zip_path)
            os.remove(txt_filename)
            print(f"BSE Deliverables Downloaded")
        else:
            print(f"BSE Deliverables Download Failed")

    def get_technical_data(technical_url, technical_path):
        response = requests.get(technical_url)
        if response.status_code == 200:
            with open(technical_path, 'wb') as file:
                file.write(response.content)
            print(f"Technicals Downloaded")
        else:
            print(f"Techniicals Download Failed")

    def get_index_data(index_url, index_path):
        response = requests.get(index_url)
        if response.status_code == 200:
            with open(index_path, "wb") as file:
                file.write(response.content)
            print("Index Downloaded")
        else:
            print(f"Index Download Failed")

    def get_daily_data(today_date=None):
        global headers
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        }

        if today_date == None:
            today = datetime.datetime.now()
        else:
            today = pd.to_datetime(today_date)
            
        day = today.day
        month = today.month
        year = today.year  

        D = today.day
        M = today.month
        Y = today.year  

        data_folder = f"{year}_{month}_{day}_Data"
        if not os.path.exists(data_folder):
            os.mkdir(data_folder)

        if month < 10:
            month = f'0{month}'
        if day < 10:
            day = f'0{day}'

        nse_bhav_url = f"https://nsearchives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_{year}{month}{day}_F_0000.csv.zip"
        nse_deli_url = f"https://nsearchives.nseindia.com/archives/equities/mto/MTO_{day}{month}{year}.DAT"
        bse_bhav_url = f"https://www.bseindia.com/download/BhavCopy/Equity/BhavCopy_BSE_CM_0_0_0_{year}{month}{day}_F_0000.CSV"
        bse_deli_url = f"https://www.bseindia.com/BSEDATA/gross/{year}/SCBSEALL{day}{month}.zip"
        technical_url = 'https://main.icharts.in/includes/screener/EODScan.php?export=1'
        index_url = f"https://archives.nseindia.com/content/indices/ind_close_all_{day}{month}{year}.csv"

        nse_zip_path = os.path.join(data_folder, f'NSE_Bhavcopy_{year}_{month}_{day}.zip')
        nse_csv_path = os.path.join(data_folder, f'NSE_Bhavcopy_{year}_{month}_{day}.csv')
        nse_deli_csv_path = os.path.join(data_folder, f'NSE_Deliverables_{year}_{month}_{day}.csv')
        bse_csv_path = os.path.join(data_folder, f'BSE_Bhavcopy_{year}_{month}_{day}.csv')
        bse_deli_zip_path = os.path.join(data_folder, f'SCBSEALL{day}{month}.zip')
        bse_deli_txt_filename = f'SCBSEALL{day}{month}.TXT'
        bse_deli_csv_path = os.path.join(data_folder, f'BSE_Deliverables_{year}_{month}_{day}.csv')
        technical_csv_path = os.path.join(data_folder, f'Technical_{year}_{month}_{day}.csv')
        index_csv_path = os.path.join(data_folder, f'Index_{year}_{month}_{day}.csv')
        
        try:
            DownloadData.get_nse_bhavcopy(nse_bhav_url, nse_zip_path, nse_csv_path)
            DownloadData.get_nse_deliverables(nse_deli_url, nse_deli_csv_path)
            DownloadData.get_bse_bhavcopy(bse_bhav_url, bse_csv_path)
            DownloadData.get_bse_deliverables(bse_deli_url, bse_deli_zip_path, bse_deli_txt_filename, bse_deli_csv_path)
            DownloadData.get_technical_data(technical_url, technical_csv_path)
            DownloadData.get_index_data(index_url, index_csv_path)

            # Compress the data folder
            shutil.make_archive(f"{data_folder}", 'zip', data_folder)

            # Delete the main data folder
            shutil.rmtree(data_folder)

            return 200
        except Exception as e:
            print(str(e))
            return 401
        

if __name__ == '__main__':
    DownloadData.get_daily_data(today_date=None)