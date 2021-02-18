import csv
from datetime import datetime

from dateutil.relativedelta import relativedelta

from src.swen344_db_utils import connect, exec_get_all, exec_commit


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
            user_id              VARCHAR(30) PRIMARY KEY NOT NULL,
            name                 VARCHAR(30) NOT NULL,
            phone_number         VARCHAR(10) NOT NULL,
            email                VARCHAR(40) NOT NULL,
            userid_set           TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            userid_reset         TIMESTAMP DEFAULT NULL,
            suspended_since      TIMESTAMP,
            suspended_till       TIMESTAMP,
            UNIQUE(email)
        );
        CREATE TABLE messages(
            message_id          SERIAL PRIMARY KEY NOT NULL,
            sender_id           VARCHAR(30) NOT NULL,
            receiver_id         VARCHAR(30) NOT NULL,
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


def rebuild_messages():
    """
    rebuild new messages table
    """
    conn = connect()
    cur = conn.cursor()
    drop_messages = """
        DROP TABLE IF EXISTS messages
    """
    create_messages = """
        CREATE TABLE messages(
            message_id          SERIAL PRIMARY KEY NOT NULL,
            sender_id           VARCHAR(30) NOT NULL,
            receiver_id         VARCHAR(30) NOT NULL,
            time_sent           TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            message             TEXT NOT NULL,
            is_read             BOOLEAN DEFAULT FALSE
        )
    """

    cur.execute(drop_messages)
    cur.execute(create_messages)
    conn.commit()
    conn.close()


def user_exists(email):
    """

    :param email:
    :return:
    """
    matches = exec_get_all('SELECT email FROM users WHERE email = %s', (email,))
    return len(matches) == 1


def get_email_by_id(user_id):
    matches = exec_get_all('SELECT email FROM users WHERE user_id = %s', (user_id,))
    return matches[0]


def create_user(user_id: str, name: str, phone_number: int, email: str, userid_set: datetime,
                userid_reset: None, suspended_since: None, suspended_till: None) -> str:
    """
    Creates a new user
    :param userid_set:
    :param userid_reset:
    :param user_id: unique id of user
    :param name: name of user
    :param email: user's email address
    :param phone_number: user's 10-digit phone number
    :param suspended_till:
    :param suspended_since:
    :return: None
    """
    conn = connect()
    cur = conn.cursor()

    if user_exists(email):
        return "User already exists"

    if len(user_id) < 6 or len(user_id) > 30:
        return "Username needs to be between 8 to 30 characters"
    if not userid_set:
        userid_set = datetime.now().strftime("%Y/%m/%d %H:%M:%S")
    string = f'INSERT INTO users(user_id, name, phone_number, email, userid_set, ' \
             f'userid_reset, suspended_since, suspended_till)' \
             f'VALUES (\'{user_id}\',\'{name}\', {phone_number},' \
             f'\'{email}\', \'{userid_set}\', NULL, NULL, NULL)'
    exec_commit(string)
    return "User added successfully"


def change_username(user_id, new_name, change_time=None):
    """
    changes username (user_id in db) if it has not been changed in the last six months
    :param user_id:
    :param new_name:
    :param change_time:
    :return:
    """
    email = get_email_by_id(user_id)

    if not user_exists(email):
        return "User doesn't exist"

    if len(new_name) < 8 or len(new_name) > 30:
        return "Username needs to be between 8 to 30 characters"

    new_time = datetime.now()
    if change_time:
        new_time = datetime.strptime(change_time, '%Y-%m-%d %H:%M:%S')

    change_date_list = exec_get_all('SELECT userid_reset FROM users WHERE email = %s', (email,))
    change_date = change_date_list[0][0]
    six_months_later = relativedelta(months=6)

    if not change_date or (change_date + six_months_later <= new_time):
        exec_commit('UPDATE users SET userid_reset = %s WHERE email = %s', (new_time, email))
        exec_commit('UPDATE users SET user_id = %s WHERE email = %s', (new_name, email))
        return "User successfully changed username to " + new_name
    return "User changed their username in the last 6 months"


def create_message(message_id: int, sender_id: str,
                   receiver_id: str, time_sent: None, message: str) -> str:
    """
    adds a message to db
    :param message_id: message id
    :param sender_id: sender's id
    :param receiver_id: receiver's id
    :param time_sent:
    :param message: text i.e. message content
    :return:
    """
    sender_email = get_email_by_id(sender_id)
    receiver_email = get_email_by_id(receiver_id)
    if not user_exists(sender_email):
        return "User" + sender_email + "doesn't exist"
    if not user_exists(receiver_email):
        return "User" + receiver_email + "doesn't exist"
    init_time = datetime.now()
    init_time = str(init_time)
    if time_sent:
        init_time = datetime.strptime(time_sent, '%Y-%m-%d %H:%M:%S')

    suspension_dates = exec_get_all('SELECT suspended_since, suspended_till FROM users WHERE email = %s',
                                    (sender_email,))
    suspended_start = suspension_dates[0][0]
    suspended_end = suspension_dates[0][1]
    if suspended_start and suspended_end:
        if suspended_start < init_time < suspended_end:
            return sender_email + " is currently suspended until " + suspended_end.strftime("%Y/%m/%d %H:%M:%S")

    # If no time was given it will default to the current time
    exec_commit(f'INSERT INTO messages(message_id, sender_id, receiver_id, time_sent, message)'
                f' VALUES (%s,%s,%s,%s,%s)',
                (message_id, sender_id, receiver_id, init_time, message))
    return "Message sent successfully"


def read_message(message_id, receiver_id):
    """
    marks message as read
    :param message_id:
    :param receiver_id:
    :return: text content
    """
    texts = exec_get_all('SELECT message FROM messages WHERE message_id = %s AND receiver_id = %s',
                             (message_id, receiver_id))
    if len(texts) == 1:
        exec_commit('UPDATE messages SET is_read = TRUE WHERE message_id = %s', (message_id,))
        return texts[0][0]


def get_unread_messages(receiver_id):
    """
    can be used to view unread texts as well as count number of unread texts
    :param receiver_id:
    :return:
    """
    if not user_exists(get_email_by_id(receiver_id)):
        return []
    unreads = exec_get_all('SELECT * FROM messages WHERE is_read = FALSE AND receiver_id = %s', (receiver_id,))
    return unreads


def get_messages_from(receiver_id, sender_id):
    """

    :param receiver_id:
    :param sender_id:
    :return:
    """
    sender_email = get_email_by_id(sender_id)
    receiver_email = get_email_by_id(receiver_id)
    if not user_exists(sender_email):
        return "User" + sender_email + "doesn't exist"
    if not user_exists(receiver_email):
        return "User" + receiver_email + "doesn't exist"
    return exec_get_all('SELECT message FROM messages WHERE sender_id = %s AND receiver_id = %s',
                        (sender_id, receiver_id))


def suspend_user(email, end_suspension, start_suspension=None):
    """
    suspends a user
    :param email:
    :param end_suspension:
    :param start_suspension:
    :return:
    """
    if not start_suspension:
        start_suspension = datetime.now().strftime("%Y/%m/%d %H:%M:%S")
    exec_commit('UPDATE users SET suspended_since = %s AND suspended_till = %s WHERE email = %s',
                (start_suspension, end_suspension, email))
    return email + " is suspended from " + start_suspension + " until " + end_suspension


def resume_user(email):
    """
    discontinues suspension for a user
    :param email:
    :return:
    """
    exec_commit('UPDATE users SET suspended_since = NULL AND suspended_till = NULL WHERE email = %s', email)
    return email + " is no longer suspended"


def get_last_message_id():
    msg_id = exec_commit('SELECT message_id FROM messages ORDER BY message_id DESC LIMIT 1')
    return msg_id


def read_csv(filename):
    """
    Reads in the who's on first csv file

    Keyword arguments:
    filename -- the whos on first csv files name (Default whos_on_first.csv)

    Return:
    None
    """
    with open(filename) as file:
        reader = csv.reader(file, delimiter=",")
        next(reader, None)
        msg_id = get_last_message_id()
        if not msg_id:
            msg_id = 1
        else:
            msg_id += 1
        for row in reader:
            if row[0] == 'Abbott':
                create_message(msg_id, 'Abbott1234', 'Costello1234', None, row[1])
            else:
                create_message(msg_id, 'Costello1234', 'Abbott1234', None, row[1])
            msg_id += 1


def populate_tables_db1():
    conn = connect()
    cur = conn.cursor()
    # check why this is necessary in CI
    rebuild_tables()
    add_users = """
                    INSERT INTO users(user_id, name, phone_number, email,
                    userid_set, userid_reset, suspended_since, suspended_till) VALUES
                        ('Abbott1234', 'Abbott', '1234567890', 'abbott@rit.edu', '1990-01-01 00:00:00',
                         NULL, NULL, NULL),
                        ('Costello1234', 'Costello', '1234567891', 'costello@rit.edu', '1990-01-01 00:00:00',
                         NULL, NULL, NULL),
                        ('Moe1234', 'Moe', '1234567892', 'moe@rit.edu', '1990-01-01 00:00:00', NULL, NULL, NULL),
                        ('Larry1234', 'Larry', '1234567893', 'larry@rit.edu', 
                        '1989-01-01 00:00:00', NULL, '1990-01-01 00:00:00', '2060-01-01 00:00:00'),
                        ('Curly1234', 'Curly', '1234567894', 'curly@rit.edu', 
                        '1989-01-01 00:00:00', NULL, '1990-01-01 00:00:00', '2000-01-01 00:00:00');
                """
    cur.execute(add_users)
    add_messages = """
                    INSERT INTO messages(message_id, sender_id, receiver_id, time_sent, message, is_read) VALUES
                        (1, 'Abbott1234', 'Costello1234', '2000-02-12 11:00:00', 'C! How are you?', TRUE),
                        (2, 'Costello1234', 'Abbott1234', '2000-02-12 12:00:40', 'Hey A! Fine. You?', TRUE),
                        (3, 'Moe1234', 'Larry1234', '1995-02-12 11:00:00', 'How are you Larry?', TRUE),
                        (4, 'Larry1234', 'Moe1234', '1995-02-12 12:00:40', 'Fine, Moe! You?', TRUE),
                        (5, 'Moe1234', 'Larry1234', '1922-02-12 12:00:40', 'I got fired.', FALSE),
                        (6, 'Costello1234', 'Abbott1234', '2020-02-12 11:00:00',
                        'Abbott! So long. How are you?', FALSE),
                        (7, 'Moe1234', 'Abbott1234', '2020-02-12 11:10:00', 'Abbott, this is Moe. Hi!', FALSE);  
                """
    cur.execute(add_messages)
    conn.commit()
    conn.close()


def populate_tables_db2():
    rebuild_tables()
    create_user('DrMarvin', 'Marvin', 5855556656, 'drmarvin@rit.edu', '1991-05-16 00:00:00', None, None, None)
    create_user('Bob12345', 'Bob', 5855654534, 'bob@rit.edu', '1991-05-17 00:00:00', None, None, None)
    create_message(1, 'Bob12345', 'DrMarvin', '1991-05-18 00:00:00', 'I\'m doing the work, I\'m baby-stepping')
    change_username('Bob12345', 'BabySteps2Door', '1991-05-19 00:00:00')
    change_username('BabySteps2Door', 'BabySteps2Elevator', '1991-05-20 00:00:00')

