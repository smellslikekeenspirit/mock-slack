import unittest
from src.chat import *
from src.swen344_db_utils import connect


class TestChat(unittest.TestCase):

    def test_build_tables(self):
        """Build the tables"""
        conn = connect()
        cur = conn.cursor()
        rebuild_tables()
        cur.execute('SELECT * FROM users')
        self.assertEqual([], cur.fetchall(), "no rows in users")
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

    # database is seeded with a test data set without crashing
    def test_populate_tables(self):
        print("Test that database is seeded with data without crashing")
        conn = connect()
        cur = conn.cursor()
        # check why this is necessary in CI
        populate_tables()
        cur.execute('SELECT * FROM users')
        self.assertEqual(5, len(cur.fetchall()), "There should be 5 rows in users table")
        cur.execute('SELECT * FROM messages')
        self.assertEqual(7, len(cur.fetchall()), "There should be 7 rows in messages table")

    # we can get all messages sent by a specific user
    def test_messages_sent_by(self):
        print("Test the messages sent by a user given a timeframe")
        conn = connect()
        cur = conn.cursor()
        populate_tables()
        cur.execute(
            'SELECT messages.message_id FROM messages INNER JOIN users ON users.user_id = messages.sender_id WHERE '
            'EXTRACT( '
            'year FROM messages.time_sent) > 1990 AND users.name = \'Costello\'')
        message_id_list = cur.fetchall()
        self.assertTrue(2 == len(message_id_list), "Costello sent 2 messages after 1990")

    # we can get all the messages between Abbott and Costello for all time
    # We can get all the messages between Moe and Larry during the year 1995
    def test_all_messages_between(self):
        print("Test retrieval of all messages between two users")
        conn = connect()
        cur = conn.cursor()
        populate_tables()
        cur.execute(
            'SELECT messages.message_id FROM messages WHERE messages.sender_id = 1 '
            'AND messages.receiver_id = 2')
        a_to_c = len(cur.fetchall())
        cur.execute(
            'SELECT messages.message_id FROM messages WHERE messages.sender_id = 2 '
            'AND messages.receiver_id = 1')
        c_to_a = len(cur.fetchall())
        self.assertTrue(3 == a_to_c + c_to_a, "Abbott and Costello exchanged 3 messages")
        print("Test retrieval of all messages between two users given a timeframe")
        cur.execute(
            'SELECT messages.message_id FROM messages WHERE messages.sender_id = 3 '
            'AND messages.receiver_id = 4 AND EXTRACT(year FROM time_sent) = 1995')
        m_to_l = len(cur.fetchall())
        cur.execute(
            'SELECT messages.message_id FROM messages WHERE messages.sender_id = 4 '
            'AND messages.receiver_id = 3 AND EXTRACT(year FROM time_sent) = 1995')
        l_to_m = len(cur.fetchall())
        self.assertTrue(2 == m_to_l + l_to_m, "Abbott and Costello exchanged 2 messages in 1995")

    # number of unread messages to Abbott is correct
    def test_unread_messages(self):
        print("Test keeping track of read/unread messages")
        conn = connect()
        cur = conn.cursor()
        populate_tables()
        cur.execute(
            'SELECT messages.message_id FROM messages WHERE messages.is_read = TRUE '
            'AND messages.receiver_id = 1')
        reads = len(cur.fetchall())
        cur.execute(
            'SELECT messages.message_id FROM messages WHERE messages.is_read = FALSE '
            'AND messages.receiver_id = 1')
        unreads = len(cur.fetchall())
        self.assertTrue(1 == reads, "Abbott has 1 read message")
        self.assertTrue(2 == unreads, "Abbott has 2 unread messages")

    # If today is May 4, 2012, then Larry is suspended
    # If today is February 29, 2000, Curly is not suspended
    def test_suspensions(self):
        """Test suspensions are correct"""
        print("Test that suspensions are in place correctly")
        conn = connect()
        cur = conn.cursor()
        populate_tables()
        larry_date = datetime(2012, 5, 4)
        curly_date = datetime(2000, 2, 29)
        cur.execute('SELECT suspended_since, suspended_till FROM users WHERE name = \'Larry\'')
        suspension_date = cur.fetchall()
        self.assertTrue((suspension_date[0][0] < larry_date) and (larry_date < suspension_date[0][1]),
                        "Larry should stay suspended on May 4 2012")
        cur.execute('SELECT suspended_till FROM users WHERE name = \'Curly\'')
        self.assertTrue(suspension_date[0][0] < curly_date, "Curly should not be suspended on February 29 2000")
        conn.close()
