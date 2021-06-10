import datetime


def to_log(text, error=''):
    with open("../log.txt", "a", encoding='utf8') as f:
        message = error + str(datetime.datetime.now()) + " : " + text + "\n"
        f.write(message)
        print(message)


def print_error(fn):
    def wrapper(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            to_log(str(e), 'Error ')
            print('Error:', e)
    return wrapper
