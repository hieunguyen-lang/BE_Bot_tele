import re
import unicodedata

def generate_invoice_key_simple(result: dict, ten_ngan_hang: str) -> str:
    """
    Tạo khóa duy nhất kiểm tra duplicate hóa đơn.
    Ưu tiên các trường gần như không thể trùng nhau trong thực tế:
    - Số hóa đơn
    - Số lô
    - Mã máy POS (TID)
    - MID
    - Ngày + Giờ giao dịch
    - Tên ngân hàng
    """
    print("[Tạo key redis]")
    def safe_get(d, key):
        return (d.get(key) or '').strip().lower()

    key = "_".join([
        safe_get(result, "sdt"),
        safe_get(result, "so_hoa_don"),
        safe_get(result, "so_lo"),
        safe_get(result, "gio_giao_dich"),
        safe_get(result, "tong_so_tien"),
        ten_ngan_hang
    ])
    print("[KEY]: ",key)
    return key

def remove_accents(s: str) -> str:
    """
    Loại bỏ dấu tiếng Việt & ký tự đặc biệt
    """
    s = unicodedata.normalize('NFD', s)
    s = s.encode('ascii', 'ignore').decode('utf-8')
    s = re.sub(r'\s+', '', s)  # bỏ toàn bộ khoảng trắng
    return s

def generate_invoice_dien(result: dict) -> str:
    print("[Tạo key redis]")

    def safe_get(d, key):
        return (d.get(key) or '').strip().lower()

    parts = [
        safe_get(result, "ten_khach_hang"),
        safe_get(result, "ma_khach_hang"),
        safe_get(result, "dia_chi"),
        safe_get(result, "so_tien"),
        safe_get(result, "ma_giao_dich"),
    ]

    parts_clean = [remove_accents(p) for p in parts]

    key = "_".join(parts_clean)

    print("[KEY]:", key)
    return key