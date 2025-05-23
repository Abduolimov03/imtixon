import psycopg2

class Database:
    def __init__(self, dbname, user, password, host='localhost', port='5432'):
        self.conn = psycopg2.connect(
            dbname=dbname, user=user, password=password, host=host, port=port
        )
        self.cur = self.conn.cursor()

        self.user = User(self)
        self.book = Book(self)
        self.comment = Comment(self)
        self.rating = Rating(self)
        self.author = Author(self)
        self.genre = Genre(self)

    def execute(self, query, params=None):
        try:
            self.cur.execute(query, params)
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            print(f"Xatolik: {e}")

    def fetchall(self):
        return self.cur.fetchall()

    def fetchone(self):
        return self.cur.fetchone()

    def close(self):
        self.cur.close()
        self.conn.close()


class User:
    def __init__(self, db: Database):
        self.db = db

    def create_user(self, full_name, email, password_hash):
        query = "INSERT INTO users (full_name, email, password) VALUES (%s, %s, %s)"
        self.db.execute(query, (full_name, email, password_hash))

    def update_user_email(self, user_id, new_email):
        query = "UPDATE users SET email = %s WHERE id = %s"
        self.db.execute(query, (new_email, user_id))

    def delete_user(self, user_id):
        self.db.execute("DELETE FROM users WHERE id = %s", (user_id,))


class Book:
    def __init__(self, db: Database):
        self.db = db

    def create_book(self, title, author_id, description, year, genre_id):
        query = """
        INSERT INTO books (title, author_id, description, published_year, genre_id)
        VALUES (%s, %s, %s, %s, %s)
        """
        self.db.execute(query, (title, author_id, description, year, genre_id))

    def update_book_title(self, book_id, new_title):
        self.db.execute("UPDATE books SET title = %s WHERE id = %s", (new_title, book_id))

    def delete_book(self, book_id):
        self.db.execute("DELETE FROM books WHERE id = %s", (book_id,))


class Comment:
    def __init__(self, db: Database):
        self.db = db

    def create_comment(self, user_id, book_id, content):
        self.db.execute("INSERT INTO comments (user_id, book_id, content) VALUES (%s, %s, %s)",
                        (user_id, book_id, content))

    def update_comment(self, comment_id, new_content):
        self.db.execute("UPDATE comments SET content = %s WHERE id = %s", (new_content, comment_id))

    def delete_comment(self, comment_id):
        self.db.execute("DELETE FROM comments WHERE id = %s", (comment_id,))


class Rating:
    def __init__(self, db: Database):
        self.db = db

    def create_rating(self, user_id, book_id, score):
        query = """
        INSERT INTO ratings (user_id, book_id, score)
        VALUES (%s, %s, %s)
        ON CONFLICT (user_id, book_id)
        DO UPDATE SET score = EXCLUDED.score, created_at = CURRENT_TIMESTAMP
        """
        self.db.execute(query, (user_id, book_id, score))

    def delete_rating(self, user_id, book_id):
        self.db.execute("DELETE FROM ratings WHERE user_id = %s AND book_id = %s", (user_id, book_id))

    # ortacha baho
    def get_average_ratings(self):
        query = """
        SELECT b.title, ROUND(AVG(r.score), 2) as avg_score
        FROM books b
        JOIN ratings r ON b.id = r.book_id
        GROUP BY b.id
        ORDER BY avg_score DESC;
        """
        self.db.execute(query)
        return self.db.fetchall()

    # kommentlar soni
    def get_comment_counts(self):
        query = """
        SELECT b.title, COUNT(c.id) AS comment_count
        FROM books b
        LEFT JOIN comments c ON b.id = c.book_id
        GROUP BY b.id
        ORDER BY comment_count DESC;
        """
        self.db.execute(query)
        return self.db.fetchall()

    # eng kop kitoblar
    def get_top_genres(self):
        query = """
        SELECT g.name AS genre_name, COUNT(b.id) AS book_count
        FROM genres g
        JOIN books b ON g.id = b.genre_id
        GROUP BY g.id
        ORDER BY book_count DESC;
        """
        self.db.execute(query)
        return self.db.fetchall()


class Author:
    def __init__(self, db: Database):
        self.db = db

    def create_author(self, full_name, country):
        self.db.execute("INSERT INTO authors (full_name, country) VALUES (%s, %s)", (full_name, country))

    def update_author(self, author_id, new_name):
        self.db.execute("UPDATE authors SET full_name = %s WHERE id = %s", (new_name, author_id))

    def delete_author(self, author_id):
        self.db.execute("DELETE FROM authors WHERE id = %s", (author_id,))


class Genre:
    def __init__(self, db: Database):
        self.db = db

    def create_genre(self, name):
        self.db.execute("INSERT INTO genres (name) VALUES (%s)", (name,))

    def update_genre(self, genre_id, new_name):
        self.db.execute("UPDATE genres SET name = %s WHERE id = %s", (new_name, genre_id))

    def delete_genre(self, genre_id):
        self.db.execute("DELETE FROM genres WHERE id = %s", (genre_id,))


if __name__ == "__main__":
    db = Database(dbname='imtixon', user='asadbek', password='asadbek')
    # db.author.create_author("Abdulla Qaxxor" , "UZB")
    # db.author.update_author(1, "Hamid olimjon")
    # db.author.delete_author(1)
    # db.user.create_user('Asadbek', 'asadbek@gmail.com', 'text' )
    # db.user.update_user_email(1, 'dfgh@gamil.com')
    print("\n Har bir kitob uchun ortacha baho:")
    for title, avg in db.rating.get_average_ratings():
        print(f" {title} ;  {avg}")

    print("\n Har bir kitob uchun kommentlar soni:")
    for title, count in db.rating.get_comment_counts():
        print(f"{title} ;  {count} ta")

    print("\n Janrlar boyicha kitoblar soni:")
    for genre, count in db.rating.get_top_genres():
        print(f"{genre} :  {count} ta kitob")

    db.close()
