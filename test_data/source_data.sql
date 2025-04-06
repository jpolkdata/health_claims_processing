CREATE TABLE sales (
    identifier INTEGER PRIMARY KEY,
    date TEXT,
    quantity INTEGER,
    price REAL
);

INSERT INTO sales (identifier, date, quantity, price) VALUES
    (1, '2024-07-01', 10, 9.99),
    (2, '2024-07-02', 15, 19.99),
    (3, '2024-07-03', 7, 14.99),
    (4, '2024-07-04', NULL, 29.99),
    (5, '2024-07-05', 20, 9.99); 