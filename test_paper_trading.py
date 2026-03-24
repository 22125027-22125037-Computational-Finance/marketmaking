import time
from datetime import datetime
from paperbroker.client import PaperBrokerClient

# ==========================================
# 1. THÔNG SỐ CẤU HÌNH (Dựa trên context.txt)
# ==========================================
CONFIG = {
    "socket_host": "papertrade.algotrade.vn",
    "socket_port": 5001,
    "rest_base_url": "https://papertrade.algotrade.vn/accounting",
    "sender_comp_id": "9629235d865343419b0dc388bbb34960",  # Lấy từ Trader ID / FIX SenderCompID
    "target_comp_id": "SERVER",
    "username": "Group08",
    "password": "FKaAWHT3Xir8",
    "sub_account": "main",  # Tham số mặc định. Lưu ý: có thể là "Group08A" tùy server setup.
    "symbol": "HNXDS:VN30F2604"  # Hợp đồng tương lai tháng gần nhất
}

def print_accounting_state(client, phase="TRẠNG THÁI"):
    """Hàm in ra tất cả tài sản, vốn (equity), và số lượng giao dịch."""
    print(f"\n{'='*50}")
    print(f"📊 {phase} ACCOUNTING STATE")
    print(f"{'='*50}")
    
    # Sử dụng context manager để gọi đúng vào sub-account
    with client.use_sub_account(CONFIG["sub_account"]):
        # 1. In Số dư và Vốn
        cash = client.get_cash_balance()
        total = client.get_account_balance()
        print(f"💰 Available Cash: {cash.get('remainCash', 0):,.0f} VND")
        print(f"🏦 Total Equity:   {total.get('totalBalance', 0):,.0f} VND")
        
        # 2. In Danh mục đầu tư (Portfolio)
        portfolio = client.get_portfolio_by_sub()
        print("\n📈 CURRENT PORTFOLIO:")
        items = portfolio.get("items", [])
        if not items:
            print("   Không có vị thế nào đang mở.")
        else:
            for pos in items:
                print(f"   {pos.get('instrument')}: {pos.get('quantity')} hợp đồng @ Giá {pos.get('avgPrice', 0):,.2f} (PnL: {pos.get('pnl', 0):,.0f})")

        # 3. In Số lượng giao dịch hôm nay
        today_str = datetime.now().strftime("%Y-%m-%d")
        # Gọi API lấy transactions bằng ngày hiện tại
        txns = client.get_transactions_by_date(today_str, today_str)
        txn_items = txns.get("items", [])
        print(f"\n🔄 NUMBER OF TRADINGS TODAY: {len(txn_items)}")

def main():
    # ==========================================
    # 2. KHỞI TẠO CLIENT & KẾT NỐI
    # ==========================================
    client = PaperBrokerClient(
        default_sub_account=CONFIG["sub_account"],
        username=CONFIG["username"],
        password=CONFIG["password"],
        rest_base_url=CONFIG["rest_base_url"],
        socket_connect_host=CONFIG["socket_host"],
        socket_connect_port=CONFIG["socket_port"],
        sender_comp_id=CONFIG["sender_comp_id"],
        target_comp_id=CONFIG["target_comp_id"],
        console=False
    )

    # Đăng ký sự kiện để theo dõi trạng thái lệnh
    client.on("fix:order:filled", lambda cl_ord_id, last_qty, last_px, **kw: print(f"   🎯 ĐÃ KHỚP LỆNH: {last_qty} @ {last_px}"))
    client.on("fix:order:rejected", lambda cl_ord_id, reason, **kw: print(f"   ❌ TỪ CHỐI LỆNH: {reason}"))

    print("🔌 Đang kết nối tới FIX Gateway...")
    client.connect()
    
    if not client.wait_until_logged_on(timeout=10):
        print(f"❌ Kết nối thất bại: {client.last_logon_error()}")
        return

    print("✅ Đã kết nối FIX Session thành công!")

    # ==========================================
    # 3. KIỂM TRA TÀI SẢN BAN ĐẦU
    # ==========================================
    print_accounting_state(client, phase="INITIAL")

    # ==========================================
    # 4. MỞ VỊ THẾ LONG (MUA 1 HỢP ĐỒNG)
    # ==========================================
    print(f"\n🚀 Đang mở vị thế LONG (MUA 1 {CONFIG['symbol']})...")
    with client.use_sub_account(CONFIG["sub_account"]):
        entry_id = client.place_order(
            full_symbol=CONFIG["symbol"],
            side="BUY",
            qty=1,
            price=0,               # Lệnh thị trường (MARKET) không cần quan tâm giá
            ord_type="MARKET"      # Khớp ngay với giá tốt nhất hiện tại
        )
    print(f"   📤 Đã gửi lệnh MUA. Mã lệnh: {entry_id}")

    # ==========================================
    # 5. GIỮ VỊ THẾ TRONG 2 PHÚT
    # ==========================================
    print("\n⏳ Đang giữ vị thế trong 2 phút (120 giây)...")
    time.sleep(120)

    # ==========================================
    # 6. ĐÓNG VỊ THẾ BẰNG LỆNH SHORT (BÁN 1 HỢP ĐỒNG)
    # ==========================================
    print(f"\n🛑 Đang đóng vị thế (BÁN 1 {CONFIG['symbol']})...")
    with client.use_sub_account(CONFIG["sub_account"]):
        exit_id = client.place_order(
            full_symbol=CONFIG["symbol"],
            side="SELL",
            qty=1,
            price=0,
            ord_type="MARKET"
        )
    print(f"   📤 Đã gửi lệnh BÁN đóng vị thế. Mã lệnh: {exit_id}")

    # Đợi vài giây cho sàn thanh toán và đồng bộ dữ liệu REST API
    print("⏳ Chờ 3 giây để hệ thống đồng bộ PnL và phí giao dịch...")
    time.sleep(3)

    # ==========================================
    # 7. KIỂM TRA TÀI SẢN SAU GIAO DỊCH & NGẮT KẾT NỐI
    # ==========================================
    print_accounting_state(client, phase="FINAL")

    print("\n🔌 Đang ngắt kết nối...")
    client.disconnect()
    print("✅ Script hoàn tất.")

if __name__ == "__main__":
    main()