import pandas as pd
import os

def process_and_split_data(raw_file_path, symbol, is_f1=True):
    """
    Hàm đọc file raw, chuẩn hóa cột và chia thành In-sample / Out-of-sample
    """
    print(f"Đang xử lý dữ liệu cho {symbol}...")
    
    # 1. Đọc dữ liệu raw
    # Lưu ý: Thay đổi các tên cột bên dưới cho khớp với file raw của bạn
    df = pd.read_csv(raw_file_path)
    
    # 2. Xử lý thời gian (Giả sử cột thời gian gốc tên là 'Time')
    # Nếu file raw không có microsecond, pandas vẫn sẽ tự thêm .000000 khi format
    df['dt_obj'] = pd.to_datetime(df['Time']) 
    df['datetime'] = df['dt_obj'].dt.strftime('%Y-%m-%d %H:%M:%S.%f')
    df['date'] = df['dt_obj'].dt.strftime('%Y-%m-%d')
    
    # 3. Gán Symbol
    df['tickersymbol'] = symbol
    
    # 4. Map các cột giá trị (Giả sử raw data có các cột: 'Price', 'Bid1', 'Ask1')
    df['price'] = df['Price']
    df['close'] = df['Price'] # Với dữ liệu tick, close thường bằng chính price
    
    if is_f1:
        df['best-bid'] = df['Bid1']
        df['best-ask'] = df['Ask1']
        df['spread'] = df['best-ask'] - df['best-bid']
        
        # Lọc đúng các cột hàm process_data cần cho F1M
        final_df = df[['date', 'datetime', 'tickersymbol', 'price', 'close', 'best-bid', 'best-ask', 'spread']]
    else:
        # Lọc đúng các cột hàm process_data cần cho F2M
        final_df = df[['date', 'datetime', 'tickersymbol', 'price', 'close']]

    # 5. Lọc và chia dữ liệu IS (2021-2023) và OS (2023-2024)
    # Dùng dt_obj để so sánh mốc thời gian cho chính xác
    mask_is = (df['dt_obj'] >= '2021-01-01') & (df['dt_obj'] < '2023-01-02')
    mask_os = (df['dt_obj'] >= '2023-01-02') & (df['dt_obj'] < '2024-01-02')
    
    df_is = final_df[mask_is]
    df_os = final_df[mask_os]
    
    return df_is, df_os

def main():
    # Tạo sẵn cấu trúc thư mục nếu chưa có
    os.makedirs('data/is', exist_ok=True)
    os.makedirs('data/os', exist_ok=True)
    
    # --- XỬ LÝ VN30F1M ---
    # Thay 'raw_F1M.csv' bằng đường dẫn tới file dữ liệu F1M gốc của bạn
    f1_is, f1_os = process_and_split_data('raw_F1M.csv', symbol='VN30F1M', is_f1=True)
    f1_is.to_csv('data/is/VN30F1M_data.csv', index=False)
    f1_os.to_csv('data/os/VN30F1M_data.csv', index=False)
    print(f"Đã lưu VN30F1M. IS: {len(f1_is)} dòng, OS: {len(f1_os)} dòng.")

    # --- XỬ LÝ VN30F2M ---
    # Thay 'raw_F2M.csv' bằng đường dẫn tới file dữ liệu F2M gốc của bạn
    f2_is, f2_os = process_and_split_data('raw_F2M.csv', symbol='VN30F2M', is_f1=False)
    f2_is.to_csv('data/is/VN30F2M_data.csv', index=False)
    f2_os.to_csv('data/os/VN30F2M_data.csv', index=False)
    print(f"Đã lưu VN30F2M. IS: {len(f2_is)} dòng, OS: {len(f2_os)} dòng.")

if __name__ == "__main__":
    main()