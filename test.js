const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
const fs = require('fs');
puppeteer.use(StealthPlugin());
const path = require('path');

function getRandomInt(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

const fakeFingerprint = {
  platform: ['Win32', 'Linux x86_64', 'MacIntel'][getRandomInt(0,2)],
  vendor: ['Google Inc.', 'Apple Computer, Inc.', ''][getRandomInt(0,2)],
  deviceMemory: getRandomInt(4, 16),
  maxTouchPoints: getRandomInt(1, 5),
  width: getRandomInt(1024, 1920),
  height: getRandomInt(768, 1080),
  devicePixelRatio: [1, 1.25, 1.5, 2][getRandomInt(0,3)]
};

(async () => {
  const browser = await puppeteer.launch({
    headless: false,
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--window-size=1280,800'
    ]
  });

  const page = await browser.newPage();

  // Fake fingerprint bằng script JS
  await page.evaluateOnNewDocument(() => {
    // Ẩn webdriver
    Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
    // Fake plugins
    Object.defineProperty(navigator, 'plugins', {
      get: () => [
        { name: "Chrome PDF Plugin" },
        { name: "Chrome PDF Viewer" },
        { name: "Native Client" }
      ]
    });
    // Fake languages
    Object.defineProperty(navigator, 'languages', {
      get: () => ['en-US', 'en']
    });
    // Fake hardwareConcurrency
    Object.defineProperty(navigator, 'hardwareConcurrency', { get: () => 4 });
    // Fake userAgent (nếu muốn)
    Object.defineProperty(navigator, 'userAgent', { get: () => "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36" });
    Object.defineProperty(navigator, 'platform', { get: () => fp.platform });
    Object.defineProperty(navigator, 'vendor', { get: () => fp.vendor });
    Object.defineProperty(navigator, 'deviceMemory', { get: () => fp.deviceMemory });
    Object.defineProperty(navigator, 'maxTouchPoints', { get: () => fp.maxTouchPoints });
    Object.defineProperty(window.screen, 'width', { get: () => fp.width });
    Object.defineProperty(window.screen, 'height', { get: () => fp.height });
    Object.defineProperty(window, 'devicePixelRatio', { get: () => fp.devicePixelRatio });
    // Vẽ con trỏ chuột ảo
  window.addEventListener('DOMContentLoaded', () => {
    const cursor = document.createElement('div');
    cursor.id = 'puppeteer-mouse';
    Object.assign(cursor.style, {
      position: 'fixed',
      top: '0px',
      left: '0px',
      width: '20px',
      height: '20px',
      background: 'rgba(0,0,0,0.3)',
      borderRadius: '50%',
      pointerEvents: 'none',
      zIndex: 9999999,
      transition: 'top 0.1s linear, left 0.1s linear'
    },fakeFingerprint);
    document.body.appendChild(cursor);

    window._moveCursor = (x, y) => {
      cursor.style.left = `${x - 10}px`;
      cursor.style.top = `${y - 10}px`;
    };
  });
  });

  // Đọc cookie từ file (dạng list)
  const cookies = JSON.parse(fs.readFileSync('test.json', 'utf8'));
  await page.setCookie(...cookies);

  // Danh sách keyword
  const keywords = [
 "giày sneaker nữ", "dép nữ", "áo khoác nữ", "áo hoodie nữ",
  "túi xách nữ", "balo", "ví da", "mũ lưỡi trai", "kính mát", "đồng hồ nữ", "vòng tay", "bông tai", "nhẫn", "dây chuyền",
  "sữa rửa mặt", "kem chống nắng", "son môi", "nước hoa", "kem dưỡng da", "mặt nạ", "sữa tắm", "dầu gội", "kem đánh răng", "nước tẩy trang",
  "bỉm", "sữa bột", "xe đẩy em bé", "ghế ăn dặm", "máy hâm sữa", "bình sữa", "đồ chơi trẻ em", "quần áo trẻ em", "giày trẻ em", "nôi em bé",
  "sách", "vở", "bút bi", "bút chì", "bút màu", "thước kẻ", "máy tính cầm tay", "balo học sinh", "đèn học", "bàn học"
];

  await page.goto('https://shopee.vn/search_user/?keyword=qu%E1%BA%A7n&page=1');
  // Tạo thư mục output nếu chưa có
  const outputDir = path.join(__dirname, 'output');
  if (!fs.existsSync(outputDir)) {
    fs.mkdirSync(outputDir);
  }
  for (const keyword of keywords) {
    const url = `https://shopee.vn/api/v4/search/search_user?keyword=${encodeURIComponent(keyword)}&limit=100&offset=0&page=search_user&with_search_cover=true`;
    try {
      const data = await page.evaluate(async (url) => {
        const res = await fetch(url, { credentials: 'include' });
        return await res.json();
      }, url);

      const filename = path.join(outputDir, `hieunk_test_${keyword}_0.json`);
      fs.writeFileSync(filename, JSON.stringify(data, null, 2));

      // Log theo dõi response
      if (data && data.data && Array.isArray(data.data.users)) {
        console.log(`[${keyword}] ✅ Số shop: ${data.data.users.length} | Đã lưu file: ${filename}`);
      } else if (data && data.error) {
        console.log(`[${keyword}] ❌ Lỗi: ${data.error} | Đã lưu file: ${filename}`);
      } else {
        console.log(`[${keyword}] ⚠️ Không có dữ liệu hợp lệ | Đã lưu file: ${filename}`);
      }
    } catch (err) {
      console.log(`[${keyword}] ❌ Exception: ${err}`);
    }

    // Nếu muốn delay giữa các keyword
    console.log('Chờ 30s...');
    await new Promise(r => setTimeout(r, 3000));
  }
  // Lưu lại cookie mới nhất
  const newCookies = await page.cookies();
  fs.writeFileSync('test.json', JSON.stringify(newCookies, null, 2));
  console.log('Đã lưu cookie mới vào test.json');

  console.log("Lấy xong dữ liệu từ Shopee thành công!");
  await browser.close();
})();
