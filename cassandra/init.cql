-- ============================================
-- Cassandra Initialization Script
-- Створює keyspace та таблицю для Wikipedia edits
-- ============================================

-- Крок 1: Створення Keyspace
CREATE KEYSPACE IF NOT EXISTS wiki_namespace
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};

-- Використовуємо створений keyspace
USE wiki_namespace;

-- Крок 2: Створення таблиці
CREATE TABLE IF NOT EXISTS edits (
    id UUID PRIMARY KEY,           -- Унікальний ідентифікатор події
    page_title TEXT,               -- Назва сторінки
    user_text TEXT,                -- Ім'я користувача
    dt TIMESTAMP                   -- Час створення сторінки
);

-- Крок 3: Створення індексу для швидшого пошуку за датою
CREATE INDEX IF NOT EXISTS edits_dt_idx ON edits (dt);

-- Крок 4: Створення індексу для пошуку за користувачем
CREATE INDEX IF NOT EXISTS edits_user_idx ON edits (user_text);

-- Виведення повідомлення про успішну ініціалізацію
-- (це буде видно в логах Cassandra)