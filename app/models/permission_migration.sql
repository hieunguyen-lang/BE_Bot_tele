-- Migration: Insert default permissions for RBAC system
INSERT INTO permissions (name, description) VALUES
('bill:view', 'Xem hóa đơn'),
('bill:create', 'Tạo hóa đơn'),
('bill:update', 'Sửa hóa đơn'),
('bill:delete', 'Xóa hóa đơn'),
('bill:export', 'Xuất hóa đơn'),
('user:create', 'Tạo người dùng'),
('user:read', 'Xem người dùng'),
('user:update', 'Sửa người dùng'),
('user:delete', 'Xóa người dùng'),
('report:summary', 'Xem báo cáo tổng hợp'),
('report:commission', 'Xem báo cáo hoa hồng'),
('report:calendar', 'Xem lịch hóa đơn'),
('view_dashboard', 'Xem dashboard'); 