import psycopg2
from psycopg2 import sql
from typing import List, Optional, Tuple, Dict, Any


def create_db(conn):
    """Создание структуры БД (таблиц)"""
    with conn.cursor() as cur:
        # Создаем таблицу клиентов
        cur.execute("""
            CREATE TABLE IF NOT EXISTS clients(
                client_id SERIAL PRIMARY KEY,
                first_name VARCHAR(50) NOT NULL,
                last_name VARCHAR(50) NOT NULL,
                email VARCHAR(50) UNIQUE
            )
        """)

        # Создаем таблицу телефонов
        cur.execute("""
            CREATE TABLE IF NOT EXISTS phones(
                phone_id SERIAL PRIMARY KEY,
                client_id INTEGER REFERENCES clients(client_id) ON DELETE CASCADE,
                phone_number VARCHAR(20) NOT NULL
            )
        """)
        conn.commit()


def add_client(conn, first_name: str, last_name: str, email: str, phones: List[str] = None) -> int:
    """Добавление нового клиента"""
    with conn.cursor() as cur:
        try:
            # Добавляем клиента
            cur.execute("""
                INSERT INTO clients(first_name, last_name, email)
                VALUES (%s, %s, %s)
                RETURNING client_id
            """, (first_name, last_name, email))
            client_id = cur.fetchone()[0]

            # Добавляем телефоны, если они указаны
            if phones:
                for phone in phones:
                    add_phone(conn, client_id, phone)

            conn.commit()
            return client_id
        except psycopg2.IntegrityError as e:
            conn.rollback()
            raise ValueError(f"Ошибка при добавлении клиента: {str(e)}")


def add_or_update_client(conn, first_name: str, last_name: str, email: str, phones: List[str] = None) -> int:
    """Добавляет нового клиента или обновляет существующего"""
    with conn.cursor() as cur:
        try:
            # Проверяем существование клиента
            cur.execute("SELECT client_id FROM clients WHERE email = %s", (email,))
            existing = cur.fetchone()

            if existing:
                # Клиент существует - обновляем
                client_id = existing[0]
                change_client(conn, client_id, first_name, last_name, email, phones)
                print(f"Обновлен существующий клиент с ID: {client_id}")
            else:
                # Клиента нет - создаем нового
                client_id = add_client(conn, first_name, last_name, email, phones)
                print(f"Добавлен новый клиент с ID: {client_id}")

            return client_id
        except psycopg2.Error as e:
            conn.rollback()
            raise ValueError(f"Ошибка при работе с клиентом: {str(e)}")


def add_phone(conn, client_id: int, phone: str) -> None:
    """Добавление телефона для существующего клиента"""
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO phones(client_id, phone_number)
            VALUES (%s, %s)
        """, (client_id, phone))
        conn.commit()


def change_client(conn, client_id: int, first_name: Optional[str] = None,
                  last_name: Optional[str] = None, email: Optional[str] = None,
                  phones: Optional[List[str]] = None) -> None:
    """Изменение данных о клиенте"""
    with conn.cursor() as cur:
        updates = []
        params = []

        if first_name is not None:
            updates.append("first_name = %s")
            params.append(first_name)
        if last_name is not None:
            updates.append("last_name = %s")
            params.append(last_name)
        if email is not None:
            updates.append("email = %s")
            params.append(email)

        if updates:
            query = sql.SQL("UPDATE clients SET {fields} WHERE client_id = %s").format(
                fields=sql.SQL(', ').join(map(sql.SQL, updates)))
            params.append(client_id)
            cur.execute(query, params)

        if phones is not None:
            cur.execute("DELETE FROM phones WHERE client_id = %s", (client_id,))
            for phone in phones:
                add_phone(conn, client_id, phone)

        conn.commit()


def delete_phone(conn, client_id: int, phone: str) -> None:
    """Удаление телефона клиента"""
    with conn.cursor() as cur:
        cur.execute("""
            DELETE FROM phones
            WHERE client_id = %s AND phone_number = %s
        """, (client_id, phone))
        conn.commit()


def delete_client(conn, client_id: int) -> None:
    """Удаление клиента"""
    with conn.cursor() as cur:
        cur.execute("DELETE FROM clients WHERE client_id = %s", (client_id,))
        conn.commit()


def find_client(conn, first_name: Optional[str] = None, last_name: Optional[str] = None,
                email: Optional[str] = None, phone: Optional[str] = None) -> List[Tuple[Any, ...]]:
    """Поиск клиента"""
    with conn.cursor() as cur:
        conditions = []
        params = []

        if first_name:
            conditions.append("c.first_name LIKE %s")
            params.append(f"%{first_name}%")
        if last_name:
            conditions.append("c.last_name LIKE %s")
            params.append(f"%{last_name}%")
        if email:
            conditions.append("c.email LIKE %s")
            params.append(f"%{email}%")

        query = """
            SELECT c.client_id, c.first_name, c.last_name, c.email, 
                   array_agg(p.phone_number) AS phones
            FROM clients c
            LEFT JOIN phones p ON c.client_id = p.client_id
        """

        if phone:
            query += """
                WHERE EXISTS (
                    SELECT 1 FROM phones p 
                    WHERE p.client_id = c.client_id AND p.phone_number LIKE %s
                )
            """
            params.insert(0, f"%{phone}%")
        elif conditions:
            query += " WHERE " + " AND ".join(conditions)

        query += " GROUP BY c.client_id"

        cur.execute(query, params)
        return cur.fetchall()


def print_client(client: Tuple[Any, ...]) -> None:
    """Печать информации о клиенте"""
    client_id, first_name, last_name, email, phones = client
    print(f"ID: {client_id}, Имя: {first_name} {last_name}, Email: {email}")
    print("Телефоны:", ', '.join(phone for phone in (phones or []) if phone))


def clear_db(conn) -> None:
    """Очистка базы данных (для тестов)"""
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS phones")
        cur.execute("DROP TABLE IF EXISTS clients")
        conn.commit()


# Пример использования
if __name__ == "__main__":
    try:
        with psycopg2.connect(
                database="clients_db",
                user="postgres",
                password="da75Tte10bA34io",
                host="localhost",
                port="5432"
        ) as conn:
            # Очищаем и создаем заново структуру БД
            clear_db(conn)
            create_db(conn)

            # Добавляем клиентов
            client1 = add_client(conn, "Катя", "Иванова", "kate@example.com", ["+79111111111", "+79112222222"])
            client2 = add_client(conn, "Данил", "Петров", "danil@example.com", ["+79113333333"])
            client3 = add_client(conn, "Сергей", "Сергеев", "sergey@example.com")

            # Добавляем телефон
            add_phone(conn, client3, "+79114444444")

            # Обновляем клиента
            change_client(conn, client1, first_name="Иван (измененный)", email="ivan_new@example.com")

            # Поиск клиентов
            print("Найдены клиенты по имени 'Иван':")
            for client in find_client(conn, first_name="Иван"):
                print_client(client)

            print("\nНайдены клиенты по телефону '333':")
            for client in find_client(conn, phone="333"):
                print_client(client)

            # Удаление данных
            delete_phone(conn, client1, "+79112222222")
            delete_client(conn, client2)

            # Вывод оставшихся клиентов
            print("\nОставшиеся клиенты:")
            for client in find_client(conn):
                print_client(client)

    except Exception as e:
        print(f"Произошла ошибка: {e}")