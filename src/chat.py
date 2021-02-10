from src.swen344_db_utils import connect
from datetime import datetime


def rebuild_tables():
    conn = connect()
    cur = conn.cursor()

    drop_example = """
            DROP TABLE IF EXISTS example_table
        """
    drop_users = """
        DROP TABLE IF EXISTS users
    """
    drop_messages = """
        DROP TABLE IF EXISTS messages
    """
    create_schema = """
        CREATE TABLE users(
            user_id              SERIAL PRIMARY KEY NOT NULL,
            name                 VARCHAR(30) NOT NULL,
            phone_number         VARCHAR(10) NOT NULL,
            email                VARCHAR(40) NOT NULL,
            suspended_since      TIMESTAMP,
            suspended_till       TIMESTAMP
        );
        CREATE TABLE messages(
            message_id          SERIAL PRIMARY KEY NOT NULL,
            sender_id           SERIAL NOT NULL,
            receiver_id         SERIAL NOT NULL,
            time_sent           TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            message             TEXT NOT NULL,
            is_read             BOOLEAN DEFAULT FALSE
        );
    """
    cur.execute(drop_example)
    cur.execute(drop_users)
    cur.execute(drop_messages)
    cur.execute(create_schema)
    conn.commit()
    conn.close()


def create_user(user_id: int, name: str, phone_number: int, email: str) -> None:
    """
    Creates a user
    :param user_id: unique id of user
    :param name: name of user
    :param email: user's email address
    :param phone_number: user's 10-digit phone number
    :return: None
    """
    conn = connect()
    cur = conn.cursor()
    string = f'INSERT INTO users(user_id, name, phone_number, email, suspended_since, suspended_till)' \
             f'VALUES ({user_id},\'{name}\', {phone_number}, \'{email}\')'
    print(string)
    cur.execute(string)


def create_message(message_id: int, sender_user_id: int, receiver_user_id: int, message: str) -> None:
    """
    adds a message to db
    :param message_id: message id
    :param sender_user_id: sender's id
    :param receiver_user_id: receiver's id
    :param message: text i.e. message content
    :return: None
    """
    conn = connect()
    cur = conn.cursor()
    dt = datetime.now()
    cur.execute(f'INSERT INTO users(message_id, sender_id, receiver_id, time_sent, message, is_read)'
                f'VALUES (\'{message_id}\', \'{sender_user_id}\', '
                f'\'{receiver_user_id}\', \'{message}\', \'{dt.now()}\')')


def populate_tables():
    conn = connect()
    cur = conn.cursor()
    # check why this is necessary in CI
    rebuild_tables()
    add_users = """
                    INSERT INTO users(user_id, name, phone_number, email, suspended_since, suspended_till) VALUES
                        (1, 'Abbott', '1234567890', 'abbott@rit.edu', NULL, NULL),
                        (2, 'Costello', '1234567891', 'costello@rit.edu', NULL, NULL),
                        (3, 'Moe', '1234567892', 'moe@rit.edu', NULL, NULL),
                        (4, 'Larry', '1234567893', 'larry@rit.edu', '1990-01-01 00:00:00', '2060-01-01 00:00:00'),
                        (5, 'Curly', '1234567894', 'curly@rit.edu', '1990-01-01 00:00:00', '2000-01-01 00:00:00');
                """
    cur.execute(add_users)
    add_messages = """
                    INSERT INTO messages(message_id, sender_id, receiver_id, time_sent, message, is_read) VALUES
                        (1, 1, 2, '2000-02-12 11:00:00', 'C! How are you?', TRUE),
                        (2, 2, 1, '2000-02-12 12:00:40', 'Hey A! Fine. You?', TRUE),
                        (3, 3, 4, '1995-02-12 11:00:00', 'How are you Larry?', TRUE),
                        (4, 4, 3, '1995-02-12 12:00:40', 'Fine, Moe! You?', TRUE),
                        (5, 3, 4, '1922-02-12 12:00:40', 'I got fired.', FALSE),
                        (6, 2, 1, '2020-02-12 11:00:00', 'Abbott! So long. How are you?', FALSE),
                        (7, 3, 1, '2020-02-12 11:10:00', 'Abbott, this is Moe. Hi!', FALSE);  
                """
    cur.execute(add_messages)
    conn.commit()
    conn.close()
