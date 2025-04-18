import psycopg2
import csv

# Подключение к базе данных PostgreSQL через Neon
conn = psycopg2.connect(
    "postgresql://neondb_owner:npg_tiQjpz6AhM0q@ep-winter-unit-a1pzep3v-pooler.ap-southeast-1.aws.neon.tech/neondb?sslmode=require"
)
cur = conn.cursor()

# Инициализация таблицы Contacts, если она ещё не существует
cur.execute("""
    CREATE TABLE IF NOT EXISTS Contacts (
        id SERIAL PRIMARY KEY,
        full_name VARCHAR(100),
        phone_number VARCHAR(20)
    )
""")
conn.commit()

# Добавление одной записи вручную (ввод имени и номера с клавиатуры)
def add_from_input():
    full_name = input("Name: ")
    phone = input("Phone number: ")
    cur.execute("INSERT INTO Contacts (full_name, phone_number) VALUES (%s, %s)", (full_name, phone))
    conn.commit()
    print("Entry added.\n")

# Добавление записей из CSV файла (с пропуском первой строки - заголовка)
def add_from_file():
    path = input("CSV file path: ")
    try:
        with open(path, newline='') as file:
            reader = csv.reader(file)
            next(reader)  # Пропустить заголовок
            for entry in reader:
                cur.execute("INSERT INTO Contacts (full_name, phone_number) VALUES (%s, %s)", entry)
        conn.commit()
        print("CSV entries uploaded.\n")
    except Exception as error:
        print("Failed to read file:", error)

# Обновление номера телефона по имени

def modify_phone():
    target_name = input("Enter name to update: ")
    new_number = input("New phone: ")
    cur.execute("UPDATE Contacts SET phone_number = %s WHERE full_name = %s", (new_number, target_name))
    conn.commit()
    print("Phone updated.\n")

# Поиск записей по имени/номеру с различными вариантами

def filter_search():
    print("Search by:")
    print("1. Name prefix")
    print("2. Exact name")
    print("3. Exact phone")
    print("4. Partial phone match")
    option = input("Choice: ")

    if option == '1':
        prefix = input("Prefix: ")
        cur.execute("SELECT * FROM Contacts WHERE full_name ILIKE %s", (prefix + '%',))
    elif option == '2':
        name = input("Exact name: ")
        cur.execute("SELECT * FROM Contacts WHERE full_name = %s", (name,))
    elif option == '3':
        phone = input("Exact number: ")
        cur.execute("SELECT * FROM Contacts WHERE phone_number = %s", (phone,))
    elif option == '4':
        part = input("Number fragment: ")
        cur.execute("SELECT * FROM Contacts WHERE phone_number LIKE %s", ('%' + part + '%',))
    else:
        print("Invalid choice.\n")
        return

    results = cur.fetchall()
    if results:
        for entry in results:
            print(entry)
    else:
        print("No match found.\n")

# Поиск по шаблону: если имя или номер содержит подстроку

def pattern_lookup():
    search = input("Name or phone contains: ")
    cur.execute("""
        SELECT * FROM Contacts
        WHERE full_name ILIKE %s OR phone_number ILIKE %s
    """, ('%' + search + '%', '%' + search + '%'))
    data = cur.fetchall()
    if data:
        for item in data:
            print(item)
    else:
        print("No entries found.\n")

# Удаление записи по имени или номеру телефона

def remove_entry():
    method = input("Delete by: (1) Name or (2) Phone? ")
    if method == '1':
        name = input("Name: ")
        cur.execute("DELETE FROM Contacts WHERE full_name = %s", (name,))
    elif method == '2':
        phone = input("Phone: ")
        cur.execute("DELETE FROM Contacts WHERE phone_number = %s", (phone,))
    conn.commit()
    print("Removed entry.\n")

# Установка всех необходимых хранимых процедур и функций в базе

def setup_functions():
    # Удаляем старые версии, если существуют
    cur.execute("DROP PROCEDURE IF EXISTS upsert_contact(TEXT, TEXT)")
    cur.execute("DROP PROCEDURE IF EXISTS bulk_insert(TEXT[][])")
    cur.execute("DROP FUNCTION IF EXISTS fuzzy_search(TEXT)")
    cur.execute("DROP FUNCTION IF EXISTS paged_query(INT, INT)")
    cur.execute("DROP PROCEDURE IF EXISTS smart_delete(TEXT)")
    conn.commit()

    # Функция нечеткого поиска
    cur.execute("""
        CREATE OR REPLACE FUNCTION fuzzy_search(keyword TEXT)
        RETURNS TABLE(id INT, full_name TEXT, phone_number TEXT)
        LANGUAGE plpgsql AS $$
        BEGIN
            RETURN QUERY SELECT * FROM Contacts
            WHERE full_name ILIKE '%' || keyword || '%'
               OR phone_number ILIKE '%' || keyword || '%';
        END;
        $$;
    """)

    # Процедура вставки или обновления записи
    cur.execute("""
        CREATE OR REPLACE PROCEDURE upsert_contact(person TEXT, contact TEXT)
        LANGUAGE plpgsql AS $$
        BEGIN
            IF EXISTS (SELECT 1 FROM Contacts WHERE full_name = person) THEN
                UPDATE Contacts SET phone_number = contact WHERE full_name = person;
            ELSE
                INSERT INTO Contacts(full_name, phone_number) VALUES (person, contact);
            END IF;
        END;
        $$;
    """)

    # Процедура массовой вставки (с проверкой валидности номеров)
    cur.execute("""
        CREATE OR REPLACE PROCEDURE bulk_insert(info TEXT[][])
        LANGUAGE plpgsql AS $$
        DECLARE
            i INT := 1;
            invalid TEXT := '';
        BEGIN
            WHILE i <= array_length(info, 1) LOOP
                IF info[i][2] ~ '^[0-9]{11}$' THEN
                    INSERT INTO Contacts(full_name, phone_number)
                    VALUES (info[i][1], info[i][2])
                    ON CONFLICT (full_name) DO UPDATE SET phone_number = EXCLUDED.phone_number;
                ELSE
                    invalid := invalid || info[i][1] || ':' || info[i][2] || ', ';
                END IF;
                i := i + 1;
            END LOOP;
            IF invalid <> '' THEN
                RAISE NOTICE 'Invalid entries: %', invalid;
            END IF;
        END;
        $$;
    """)

    # Функция постраничного просмотра
    cur.execute("""
        CREATE OR REPLACE FUNCTION paged_query(limit_val INT, offset_val INT)
        RETURNS TABLE(id INT, full_name TEXT, phone_number TEXT)
        LANGUAGE sql AS $$
        SELECT * FROM Contacts ORDER BY id LIMIT limit_val OFFSET offset_val;
        $$;
    """)

    # Процедура умного удаления по имени или номеру
    cur.execute("""
        CREATE OR REPLACE PROCEDURE smart_delete(value TEXT)
        LANGUAGE plpgsql AS $$
        BEGIN
            DELETE FROM Contacts WHERE full_name = value OR phone_number = value;
        END;
        $$;
    """)

    conn.commit()
    print("Database procedures and functions set up.\n")

# Вызов процедуры upsert (обновление или добавление записи)
def exec_upsert():
    name = input("User name: ")
    number = input("Phone: ")
    cur.execute("CALL upsert_contact(%s, %s)", (name, number))
    conn.commit()
    print("Upsert done.\n")

# Вызов процедуры bulk_insert для пакетной вставки

def exec_bulk_insert():
    count = int(input("Number of records: "))
    users = []
    for i in range(count):
        name = input(f"Name {i+1}: ")
        phone = input(f"Phone {i+1}: ")
        users.append([str(i+1), name, phone])
    cur.execute("CALL bulk_insert(%s)", (users,))
    conn.commit()
    print("Bulk insert complete.\n")

# Вызов функции постраничного просмотра

def exec_paginated_query():
    lim = int(input("Limit: "))
    off = int(input("Offset: "))
    cur.execute("SELECT * FROM paged_query(%s, %s)", (lim, off))
    results = cur.fetchall()
    for row in results:
        print(row)
    print()

# Вызов процедуры smart_delete

def exec_delete_proc():
    keyword = input("Enter name or phone to delete: ")
    cur.execute("CALL smart_delete(%s)", (keyword,))
    conn.commit()
    print("Deletion done via procedure.\n")

# Меню взаимодействия с пользователем
while True:
    print("== Contact Book ==")
    print("1. Add entry manually")
    print("2. Load from CSV")
    print("3. Modify phone number")
    print("4. Search entries")
    print("5. Delete entry")
    print("6. Pattern-based search")
    print("7. Setup DB procedures")
    print("8. Upsert one entry")
    print("9. Bulk insert entries")
    print("10. Paginated view")
    print("11. Delete using procedure")
    print("12. Exit")

    action = input("Choose option: ")
    print()

    if action == '1':
        add_from_input()
    elif action == '2':
        add_from_file()
    elif action == '3':
        modify_phone()
    elif action == '4':
        filter_search()
    elif action == '5':
        remove_entry()
    elif action == '6':
        pattern_lookup()
    elif action == '7':
        setup_functions()
    elif action == '8':
        exec_upsert()
    elif action == '9':
        exec_bulk_insert()
    elif action == '10':
        exec_paginated_query()
    elif action == '11':
        exec_delete_proc()
    elif action == '12':
        break
    else:
        print("Invalid input.\n")

# Закрытие соединения
cur.close()
conn.close()