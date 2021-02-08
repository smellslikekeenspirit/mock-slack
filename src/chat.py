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
            email                VARCHAR(40) NOT NULL
        );
        CREATE TABLE messages(
            message_id          SERIAL PRIMARY KEY NOT NULL,
            sender_user_id      SERIAL NOT NULL,
            receiver_user_id    SERIAL NOT NULL,
            message             TEXT NOT NULL,
            time_sent           TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
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
    string = f'INSERT INTO users(user_id, name, phone_number, email) VALUES ({user_id},\'{name}\',' \
             f'{phone_number}, \'{email}\')'
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
    cur.execute(f'INSERT INTO users(message_id, sender_user_id, receiver_user_id, message, time_sent)'
                f'VALUES (\'{message_id}\', \'{sender_user_id}\', '
                f'\'{receiver_user_id}\', \'{message}\', \'{dt.now()}\')')
