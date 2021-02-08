import unittest
from src.chat import *
from src.swen344_db_utils import connect


class TestChat(unittest.TestCase):

    def test_build_tables(self):
        """Build the tables"""
        conn = connect()
        cur = conn.cursor()
        rebuild_tables()
        cur.execute('SELECT * FROM example_table')
        self.assertEqual([], cur.fetchall(), "no rows in example_table")
        conn.close()

    def test_rebuild_tables_is_idempotent(self):
        """Drop and rebuild the tables twice"""
        rebuild_tables()
        rebuild_tables()
        conn = connect()
        cur = conn.cursor()
        cur.execute('SELECT * FROM users')
        self.assertEqual([], cur.fetchall(), "no rows in users")
        conn.close()

    def test_add_users(self):
        conn = connect()
        cur = conn.cursor()
        add_users = """
                INSERT INTO users(user_id, name, phone_number, email) VALUES
                    (1, 'Prionti', 5855689678, 'pdn3628@rit.edu'),
                    (2, 'John', 5855689679, 'jp2345@rit.edu');
            """
        cur.execute(add_users)
        # create_user(1, 'Prionti', 5855689678, 'pdn3628@rit.edu')
        # create_user(2, 'John', 5855689679, 'jp2345@rit.edu')
        cur.execute('SELECT * FROM users')
        self.assertEqual(2, len(cur.fetchall()), "two rows in users table")

    def test_add_messages(self):
        conn = connect()
        cur = conn.cursor()
        add_messages = """
                INSERT INTO messages(message_id, sender_user_id, receiver_user_id, message, time_sent) VALUES
                    (1, 1, 2, 'How are you?', '2021-02-12 11:00:00'),
                    (2, 2, 1, 'Fine! You?', '2021-02-12 12:00:40');
            """
        cur.execute(add_messages)
        # create_user(1, 'Prionti', 5855689678, 'pdn3628@rit.edu')
        # create_user(2, 'John', 5855689679, 'jp2345@rit.edu')
        cur.execute('SELECT * FROM messages')
        self.assertEqual(2, len(cur.fetchall()), "two rows in messages table")

    def test_get_by_phone_number(self):
        """Test the contact information"""
        print("Test the contact information")
        conn = connect()
        cur = conn.cursor()
        add_users = """
                        INSERT INTO users(user_id, name, phone_number, email) VALUES
                            (1, 'Prionti', 5855689678, 'pdn3628@rit.edu'),
                            (2, 'John', 5855689679, 'jp2345@rit.edu');
                    """
        cur.execute(add_users)
        cur.execute('SELECT * FROM users WHERE phone_number = \'5855689679\'')
        user = cur.fetchall()
        self.assertTrue('John' == user[0][1], "5855689679 is John's phone number")


