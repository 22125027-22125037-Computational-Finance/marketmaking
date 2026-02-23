import requests
import pandas as pd
import os
import time
from datetime import datetime

def fetch_dnse_api(symbol, start_date, end_date, is_f1=True):
    print(f"\nĐang gọi API DNSE tải {symbol} từ {start_date} đến {end_date}...")
    
    # Chuyển đổi ngày sang Unix Timestamp (giây) mà API yêu cầu
    start_ts = int(datetime.strptime(start_date, '%Y-%m-%d').timestamp())
    end_ts = int(datetime.strptime(end_date, '%Y-%m-%d').timestamp())
    
    url = "https://services.entrade.com.vn/chart-api/v2/ohlcs/derivative"
    all_chunks = []
    
    # Chia nhỏ request mỗi 30 ngày để API không bị quá tải
    current_ts = start_ts
    while current_ts < end_ts:
        next_ts = min(current_ts + 30 * 24 * 60 * 60, end_ts)
        
        params = {
            'from': current_ts,
            'to': next_ts,
            'symbol': symbol,
            'resolution': 1 # Khung 1 phút
        }
        
        try:
            response = requests.get(url, params=params)
            data = response.json()
            
            if 't' in data and len(data['t']) > 0:
                df_chunk = pd.DataFrame({
                    'timestamp': data['t'],
                    'close': data['c']
                })
                all_chunks.append(df_chunk)
                
                chunk_start_str = datetime.fromtimestamp(current_ts).strftime('%Y-%m-%d')
                print(f"  -> Đã lấy đoạn từ {chunk_start_str}: {len(data['t'])} dòng.")
            else:
                print(f"  -> Không có dữ liệu đoạn {datetime.fromtimestamp(current_ts).strftime('%Y-%m-%d')}.")
        except Exception as e:
            print(f"  [!] Lỗi call API: {e}")
            
        current_ts = next_ts
        time.sleep(0.5) # Nghỉ nửa giây giữa các request
        
    if not all_chunks:
        return pd.DataFrame()
        
    # Gộp data và xử lý trùng lặp
    df = pd.concat(all_chunks, ignore_index=True)
    df = df.drop_duplicates(subset=['timestamp'])
    
    # Chuyển đổi Unix timestamp về datetime chuẩn của hệ thống
    df['dt_obj'] = pd.to_datetime(df['timestamp'], unit='s', utc=True).dt.tz_convert('Asia/Ho_Chi_Minh').dt.tz_localize(None)
    df['datetime'] = df['dt_obj'].dt.strftime('%Y-%m-%d %H:%M:%S.%f')
    df['date'] = df['dt_obj'].dt.strftime('%Y-%m-%d')
    df['tickersymbol'] = symbol
    df['price'] = df['close']
    
    # Giả lập sổ lệnh
    if is_f1:
        df['best-bid'] = df['close'] - 0.1
        df['best-ask'] = df['close'] + 0.1
        df['spread'] = df['best-ask'] - df['best-bid']
        return df[['date', 'datetime', 'tickersymbol', 'price', 'close', 'best-bid', 'best-ask', 'spread', 'dt_obj']]
    else:
        return df[['date', 'datetime', 'tickersymbol', 'price', 'close', 'dt_obj']]

def main():
    os.makedirs('data/is', exist_ok=True)
    os.makedirs('data/os', exist_ok=True)
    
    start_str = '2025-11-01'
    end_str = '2026-02-20'
    
    f1_data = fetch_dnse_api('VN30F1M', start_str, end_str, is_f1=True)
    f2_data = fetch_dnse_api('VN30F2M', start_str, end_str, is_f1=False)
    
    if f1_data.empty or f2_data.empty:
        print("Dữ liệu trống, dừng xử lý.")
        return

    print("\nĐang lưu dữ liệu...")
    
    is_mask_f1 = (f1_data['dt_obj'] >= '2025-11-01') & (f1_data['dt_obj'] < '2026-01-01')
    os_mask_f1 = (f1_data['dt_obj'] >= '2026-01-01') & (f1_data['dt_obj'] <= '2026-02-20')
    
    f1_data[is_mask_f1].drop(columns=['dt_obj']).to_csv('data/is/VN30F1M_data.csv', index=False)
    f1_data[os_mask_f1].drop(columns=['dt_obj']).to_csv('data/os/VN30F1M_data.csv', index=False)
    
    is_mask_f2 = (f2_data['dt_obj'] >= '2025-11-01') & (f2_data['dt_obj'] < '2026-01-01')
    os_mask_f2 = (f2_data['dt_obj'] >= '2026-01-01') & (f2_data['dt_obj'] <= '2026-02-20')
    
    f2_data[is_mask_f2].drop(columns=['dt_obj']).to_csv('data/is/VN30F2M_data.csv', index=False)
    f2_data[os_mask_f2].drop(columns=['dt_obj']).to_csv('data/os/VN30F2M_data.csv', index=False)

    print(f"Xong! VN30F1M (IS: {is_mask_f1.sum()}, OS: {os_mask_f1.sum()})")
    print(f"Xong! VN30F2M (IS: {is_mask_f2.sum()}, OS: {os_mask_f2.sum()})")

if __name__ == "__main__":
    main()