import csv
import string
from datetime import datetime

from dateutil.relativedelta import relativedelta

from src.swen344_db_utils import connect, exec_get_all, exec_commit


def rebuild_tables():
    """
    drops tables if necessary and builds all the tables from scratch
    :return:
    """
    conn = connect()
    cur = conn.cursor()
    drop_users = """
            DROP TABLE IF EXISTS users
        """
    drop_communities = """
            DROP TABLE IF EXISTS communities
        """
    drop_memberships = """
            DROP TABLE IF EXISTS memberships
        """
    drop_channels = """
            DROP TABLE IF EXISTS channels
        """
    drop_channel_posts = """
            DROP TABLE IF EXISTS channel_posts
        """
    drop_unread_posts = """
            DROP TABLE IF EXISTS unread_posts
        """
    drop_mentions = """
            DROP TABLE IF EXISTS mentions
        """
    drop_direct_messages = """
            DROP TABLE IF EXISTS direct_messages
        """
    drop_suspensions = """
            DROP TABLE IF EXISTS suspensions
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
        CREATE TABLE direct_messages(
            message_id          SERIAL PRIMARY KEY NOT NULL,
            sender_id           VARCHAR(30) NOT NULL,
            receiver_id         VARCHAR(30) NOT NULL,
            time_sent           TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            message             TEXT NOT NULL,
            is_read             BOOLEAN DEFAULT FALSE
        );
        CREATE TABLE communities(
            name     VARCHAR(40) PRIMARY KEY NOT NULL
        );
        CREATE TABLE memberships(
            user_id        VARCHAR(30) NOT NULL,
            community_name VARCHAR(40) NOT NULL,
            PRIMARY KEY(user_id, community_name)
        );
        CREATE TABLE unread_posts(
            user_id      VARCHAR(30) NOT NULL,
            post_id      VARCHAR(40) NOT NULL,
            PRIMARY KEY(user_id, post_id)
        );
        CREATE TABLE channels(
            id             SERIAL PRIMARY KEY,
            name           VARCHAR(40) NOT NULL,
            community_name VARCHAR(40) NOT NULL,
            UNIQUE(name, community_name)
        );
        CREATE TABLE channel_posts(
            id           SERIAL PRIMARY KEY NOT NULL,
            channel_id   INT NOT NULL,
            text         TEXT NOT NULL,
            user_id      VARCHAR(30) NOT NULL,
            time_sent    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE mentions(
            user_id      VARCHAR(30) NOT NULL,
            post_id      VARCHAR(40) NOT NULL,
            PRIMARY KEY(user_id, post_id)
        );
        CREATE TABLE suspensions(
            user_id         VARCHAR(30) NOT NULL,
            suspended_since TIMESTAMP DEFAULT NULL,
            suspended_till  TIMESTAMP DEFAULT NULL,
            community_name  VARCHAR(40) NOT NULL,
            PRIMARY KEY(user_id, community_name)
        ); 
    """
    cur.execute(drop_users)
    cur.execute(drop_direct_messages)
    cur.execute(drop_communities)
    cur.execute(drop_channels)
    cur.execute(drop_channel_posts)
    cur.execute(drop_memberships)
    cur.execute(drop_unread_posts)
    cur.execute(drop_mentions)
    cur.execute(drop_suspensions)
    cur.execute(create_schema)
    conn.commit()
    conn.close()


def rebuild_direct_messages():
    conn = connect()
    cur = conn.cursor()
    drop_direct_messages = """
            DROP TABLE IF EXISTS direct_messages
        """
    create_direct_messages = """
            CREATE TABLE direct_messages(
                message_id          SERIAL PRIMARY KEY NOT NULL,
                sender_id           VARCHAR(30) NOT NULL,
                receiver_id         VARCHAR(30) NOT NULL,
                time_sent           TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                message             TEXT NOT NULL,
                is_read             BOOLEAN DEFAULT FALSE
            );
        """
    cur.execute(drop_direct_messages)
    cur.execute(create_direct_messages)
    conn.commit()
    conn.close()


def user_exists(email):
    """
    checks if a user exists in the system by email
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


def community_exists(community_name):
    matching_communities = exec_get_all('SELECT name FROM communities WHERE name = %s', (community_name,))
    return len(matching_communities) == 1


def channel_exists(channel_name, community_name):
    matching_channels = exec_get_all('SELECT id FROM channels WHERE name = %s AND community_name = %s',
                                     (channel_name, community_name))
    return len(matching_channels) == 1


def add_channel(channel, community_name):
    if channel_exists(channel, community_name):
        return channel + " exists"

    if not community_exists(community_name):
        return "The community doesn't exist"

    exec_commit('INSERT INTO channels (name,community_name) VALUES (%s,%s)',
                (channel, community_name))
    return channel + " was added to " + community_name


def add_community(community_name, channels=[]):
    """
    This function will add a new community and a list of channels to it if specified
    """
    if community_exists(community_name):
        return "The community already exists"

    exec_commit('INSERT INTO communities (name) VALUES (%s)',
                (community_name,))
    print("Creating community " + community_name)
    for channel in channels:
        print(add_channel(channel, community_name) + " ")
    return community_name + " was added "


def add_user_to_community(user_id, community):
    """
    This will add a user to a community
    """
    if not community_exists(community):
        return "The community doesn't exist"
    if not user_exists(get_email_by_id(user_id)):
        return "The user doesn't exist"
    exec_commit('INSERT INTO memberships (user_id,community_name) VALUES (%s,%s)',
                (user_id, community))
    return user_id + " is now a member of " + community


def get_users_in_community(community):
    """
    This function will get the list of all users that are in the given channel
    """
    if not community_exists(community):
        return []

    user_list = exec_get_all('SELECT user_id FROM memberships WHERE community_name = %s', (community,))

    return [user[0] for user in user_list]


def post_to_channel(poster_id, channel, community, message,
                    time_sent=datetime.now().strftime("%Y-%m-%d %H:%M:%S")):
    """
    This function will let a user send a message on a channel
    """
    if not (channel_exists(channel, community)):
        return "The channel doesn't exist"
    community_ids = get_users_in_community(community)
    if not poster_id in community_ids:
        return "User is not a part of the community"
    if is_suspended(poster_id, channel, time_sent):
        return "User is suspended"
    mentioned_users = []
    split_message = message.split("@")

    for i in range(len(split_message)):
        if i % 2 == 1:
            first_word_after_symbol = split_message[i].translate(string.punctuation).split()[0]
            for user_id in community_ids:
                if user_id == first_word_after_symbol:
                    mentioned_users.append(user_id)
                    break
    mentioned_users = list(set(mentioned_users))
    community_ids.remove(poster_id)
    conn = connect()
    cur = conn.cursor()
    cur.execute('SELECT id FROM channels WHERE community_name = %s AND name = %s', (community, channel))
    channel_id = cur.fetchall()[0][0]
    cur.execute('INSERT INTO channel_posts (channel_id, text, user_id, time_sent) VALUES (%s,%s,%s,%s) RETURNING id',
                (channel_id, message, poster_id, time_sent))
    post_id = cur.fetchall()[0][0]
    for user_id in community_ids:
        cur.execute('INSERT INTO unread_posts (user_id,post_id) VALUES (%s,%s)',
                    (user_id, post_id))

    for user_id in mentioned_users:
        cur.execute('INSERT INTO mentions (user_id,post_id) VALUES (%s,%s)',
                    (user_id, post_id))
    conn.commit()
    conn.close()
    return "Message sent to channel"


def create_direct_message(message_id: int, sender_id: str,
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
    suspended_since = suspension_dates[0][0]
    suspended_till = suspension_dates[0][1]
    if suspended_since and suspended_till:
        if suspended_since < init_time < suspended_till:
            return sender_email + " is currently suspended until " + suspended_till.strftime("%Y/%m/%d %H:%M:%S")

    # If no time was given it will default to the current time
    exec_commit(f'INSERT INTO direct_messages(message_id, sender_id, receiver_id, time_sent, message)'
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
    texts = exec_get_all('SELECT message FROM direct_messages WHERE message_id = %s AND receiver_id = %s',
                         (message_id, receiver_id))
    if len(texts) == 1:
        exec_commit('UPDATE direct_messages SET is_read = TRUE WHERE message_id = %s', (message_id,))
        return texts[0][0]


def get_unread_messages(receiver_id):
    """
    can be used to view unread texts as well as count number of unread texts
    :param receiver_id:
    :return:
    """
    if not user_exists(get_email_by_id(receiver_id)):
        return []
    unreads = exec_get_all('SELECT * FROM direct_messages WHERE is_read = FALSE AND receiver_id = %s', (receiver_id,))
    return unreads


def get_messages_from(receiver_id, sender_id):
    """
    this will return messages between two given people
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
    return exec_get_all('SELECT message FROM direct_messages WHERE sender_id = %s AND receiver_id = %s',
                        (sender_id, receiver_id))


def get_unread_posts(user_id):
    """
    This function will return the list of all unread posts
    """
    if not user_exists(get_email_by_id(user_id)):
        return [], 0
    unread_list = exec_get_all('SELECT post_id FROM unread_posts WHERE user_id = %s', (user_id,))
    return [message_id[0] for message_id in unread_list], len(unread_list)


def get_mentions(user_id):
    """
    This will get a list of all unread post and return the list and the number of them
    """
    if not user_exists(get_email_by_id(user_id)):
        return [], 0
    mention_list = exec_get_all('SELECT * FROM mentions WHERE user_id = %s', (user_id,))
    return [message_id[0] for message_id in mention_list], len(mention_list)


def suspend_user(user_id, community_name, end_suspension, start_suspension=None):
    """
    suspends a user from a commmunity
    :param user_id:
    :param community_name:
    :param end_suspension:
    :param start_suspension:
    :return:
    """
    if not user_exists(get_email_by_id(user_id)):
        return "User doesn't exist"
    if not community_exists(community_name):
        return "Community does not exist"

    if not start_suspension:
        start_suspension = datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S")
    suspension_list = exec_commit('SELECT user_id FROM suspensions WHERE user_id = %s AND community_name = %s',
                                  (user_id, community_name))
    if not suspension_list:
        exec_commit('INSERT INTO suspensions (user_id, suspended_since, suspended_till, community_name) VALUES(%s, '
                    '%s, %s, %s)', (user_id, start_suspension, end_suspension, community_name))
    else:
        exec_commit('UPDATE suspensions SET suspended_since = %s AND suspended_till = %s WHERE user_id = %s AND '
                    'community_name = %s', (start_suspension, end_suspension, user_id, community_name))
    return user_id + " is suspended from " + start_suspension + " until " + end_suspension + " on " + community_name


def resume_user(user_id, community_name):
    """
    discontinues suspension for a user
    :param user_id:
    :return:
    """
    if not user_exists(get_email_by_id(user_id)):
        return "User doesn't exist"
    if not community_exists(community_name):
        return "Community does not exist"
    exec_commit(
        'UPDATE suspensions SET suspended_since = NULL AND suspended_till = NULL'
        ' WHERE user_id = %s AND community_name = %s',
        (user_id, community_name))
    return user_id + " is no longer suspended on " + community_name


def is_suspended(email, channel_name, sending_time):
    """
    This will tell us if the user is suspended or not on the channel
    """
    sending_time = datetime.strptime(sending_time, '%Y-%m-%d %H:%M:%S')
    conn = connect()
    cur = conn.cursor()
    cur.execute('SELECT community_name FROM channels WHERE name = %s', (channel_name,))
    community_name = cur.fetchall()[0][0]
    cur.execute('SELECT suspended_since, suspended_till FROM suspensions WHERE user_id = %s AND community_name = %s',
                (email, community_name))
    suspension_dates = cur.fetchall()
    conn.close()
    if not suspension_dates:
        return False
    suspended_since = suspension_dates[0][0]
    suspended_till = suspension_dates[0][1]
    if suspended_since and suspended_till:
        if suspended_since < sending_time < suspended_till:
            return True
    return False


def get_last_message_id():
    """
    this will get us the last sent direct message on the app
    :return:
    """
    msg_id = exec_commit('SELECT message_id FROM direct_messages ORDER BY message_id DESC LIMIT 1')
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
                create_direct_message(msg_id, 'Abbott1234', 'Costello1234', None, row[1])
            else:
                create_direct_message(msg_id, 'Costello1234', 'Abbott1234', None, row[1])
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
                    INSERT INTO direct_messages(message_id, sender_id, receiver_id, time_sent, message, is_read) VALUES
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
    create_direct_message(1, 'Bob12345', 'DrMarvin', '1991-05-18 00:00:00', 'I\'m doing the work, I\'m baby-stepping')
    change_username('Bob12345', 'BabySteps2Door', '1991-05-19 00:00:00')
    change_username('BabySteps2Door', 'BabySteps2Elevator', '1991-05-20 00:00:00')


def populate_tables_db3():
    rebuild_tables()
    populate_tables_db1()
    create_user('DrMarvin', 'Marvin', 5855556656, 'drmarvin@rit.edu', '1991-05-16 00:00:00', None, None, None)
    create_user('Bob12345', 'Bob', 5855654534, 'bob@rit.edu', '1991-05-17 00:00:00', None, None, None)
    add_community('Metropolis', ['DailyPlanet', 'Random'])
    add_community('Comedy', ['ArgumentClinic', 'Dialogs'])
    user_list = exec_get_all('SELECT user_id FROM users')
    for user in user_list:
        add_user_to_community(user[0], 'Comedy')
    create_user('clarknotsuperman', 'Clark', 5855556434, 'clark@rit.edu', '1991-05-16 00:00:00', None, None, None)
    create_user('lex12345', 'Lex', 5855556234, 'lex@rit.edu', '1991-05-16 00:00:00', None, None, None)
    add_user_to_community('clarknotsuperman', 'Metropolis')
    create_direct_message(8, 'lex12345', 'Moe1234', '1991-05-18 00:00:00', 'Hi Moe!')
    create_direct_message(9, 'Moe1234', 'lex12345', '1991-05-18 00:00:10', 'Hi Lex!')
