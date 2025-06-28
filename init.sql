CREATE DATABASE IF NOT EXISTS bill_data CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE bill_data;

CREATE TABLE IF NOT EXISTS thong_tin_hoa_don (
    id INT AUTO_INCREMENT PRIMARY KEY,
    thoi_gian DATETIME,
    nguoi_gui VARCHAR(255),
    ten_khach VARCHAR(255),
    so_dien_thoai VARCHAR(20),
    type_dao_rut VARCHAR(20), 
    ngan_hang VARCHAR(100),
    ngay_giao_dich VARCHAR(50),
    gio_giao_dich VARCHAR(50),
    tong_so_tien VARCHAR(50),
    so_the VARCHAR(100),
    tid VARCHAR(50),
    mid VARCHAR(50),
    so_lo VARCHAR(100),
    so_hoa_don VARCHAR(100),
    ten_may_pos VARCHAR(100),
    lich_canh_bao VARCHAR(50),
    tien_phi VARCHAR(50),
    batch_id VARCHAR(250),
    caption_goc TEXT,
    ket_toan VARCHAR(255),
    phi_pos VARCHAR(255),
    phi_thu_khach VARCHAR(255),
    ck_khach_rut VARCHAR(255),
    tien_ve_tk_cty VARCHAR(255),
    tinh_trang VARCHAR(255),
    lenh_treo VARCHAR(255),
    ly_do VARCHAR(255)
);




CREATE TABLE IF NOT EXISTS users
(
    id              int auto_increment
        primary key,
    created_at      datetime default CURRENT_TIMESTAMP null,
    updated_at      datetime default CURRENT_TIMESTAMP null,
    email           varchar(255)                       null,
    username        varchar(255)                       null,
    hashed_password varchar(255)                       null,
    role            varchar(255)                       null,
    is_active       tinyint(1)                         null,
    constraint ix_users_email
        unique (email),
    constraint ix_users_username
        unique (username)
);

create index ix_users_id
    on users (id);
