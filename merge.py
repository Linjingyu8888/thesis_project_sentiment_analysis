import csv
import sqlite3
from typing import List
import dateparser


def main():
    b_lines = read_B()
    m_lines = read_M()
    t_lines = read_T()

    print('cleaning...')
    b_clean_lines = clean(b_lines)
    m_clean_lines = clean(m_lines)
    t_clean_lines = clean(t_lines)

    save_to_sqlite_db(b_clean_lines, m_clean_lines, t_clean_lines)


def save_to_sqlite_db(b_clean_lines, m_clean_lines, t_clean_lines):
    """this function takes all the clean lines and saves them into sqlite database file"""
    print('saving to db...')
    db_file = 'restaurants.db'
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS reviews (
                                        id integer PRIMARY KEY,
                                        restaurant_name text NOT NULL,
                                        visit_date text NOT NULL,
                                        comment text NOT NULL
                                    );
    """
    insert_row_sql = "INSERT INTO reviews(restaurant_name,visit_date,comment) VALUES (?,?,?)"
    connexion = None
    try:
        connexion = sqlite3.connect(db_file)
        c = connexion.cursor()
        c.execute(create_table_sql)
        for line in b_clean_lines:
            c.execute(insert_row_sql, ('By Chloe', line[0], line[1]))
        for line in m_clean_lines:
            c.execute(insert_row_sql, ('Mildreds Soho', line[0], line[1]))
        for line in t_clean_lines:
            c.execute(insert_row_sql, ('Tibits', line[0], line[1]))
        connexion.commit()
    except sqlite3.Error as e:
        print(e)
    finally:
        if connexion:
            connexion.close()


def clean(dirty_lines):
    clean_lines = []
    last_known_good_date = ''
    for line in dirty_lines:
        date = line[0].replace('Date of visit: ', '').replace('>', '')
        comment = line[1]
        if date.strip().startswith('<img src'):
            date = ''
        if date.strip() == '' and comment.strip() == '':
            pass  # we dont save empty line
        else:
            months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
            if date.strip() == '' or (len(date.strip()) >= 3 and date.strip()[0:3] not in months):
                date = last_known_good_date
            else:
                last_known_good_date = date.strip()
            parsed_date = dateparser.parse(date)
            if parsed_date is None:
                print("date is empty and cannot be fixed. comment not saved")
            else:
                clean_lines.append([parsed_date.date(), comment])
    return clean_lines


def read_B():
    print('reading B...')
    part_1_name = "B.csv"
    part_1_lines = read_csv(part_1_name)

    merged_lines = []
    for line in part_1_lines:
        date = line[3]
        comment = line[5]
        merged_lines.append([date, comment])
    return merged_lines


def read_M():
    print('reading M...')
    part_1_name = "9-10-M-2-79.csv"
    part_2_name = "9-11-M-80-205.csv"

    part_1_lines = read_csv(part_1_name)
    part_2_lines = read_csv(part_2_name)

    merged_lines = []
    for line in part_1_lines:
        date = line[1]
        comment = line[2]
        merged_lines.append([date, comment])
    for line in part_2_lines:
        date = line[3]
        comment = line[5]
        merged_lines.append([date, comment])

    return merged_lines


def read_T():
    print('reading T...')
    part_1_name: str = "9-10-T-2-26.csv"
    part_2_name = "9-10-T-27-56.csv"
    part_3_name = "9-10-T-57-143.csv"

    part_1_lines: List[List[str]] = read_csv(part_1_name)
    part_2_lines = read_csv(part_2_name)
    part_3_lines = read_csv(part_3_name)

    merged_lines = []
    for line in part_1_lines:
        date = line[3]
        comment = line[5]
        merged_lines.append([date, comment])
    for line in part_2_lines:
        date = line[3]
        comment = line[5]
        merged_lines.append([date, comment])
    for line in part_3_lines:
        date = line[2]
        comment = line[-1]
        merged_lines.append([date, comment])

    return merged_lines


def read_csv(part_name: str) -> List[List[str]]:
    part_lines = []
    with open(part_name) as csv_file:
        reader = csv.reader(csv_file, delimiter=',', quotechar='"')

        for line in reader:
            part_lines.append(line)
    part_lines.pop(0)
    return part_lines


if __name__ == '__main__':
    main()
