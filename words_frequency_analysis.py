import re
import sqlite3
import string
from typing import List

import nltk
from nltk.corpus import stopwords
from nltk.tag import pos_tag
import matplotlib.pyplot as plt


def main():
    db_name = 'restaurants.db'
    # generate_data_for_histograms(db_name)

    word_frequency_analysis(db_name)


def word_frequency_analysis(db_name):
    nltk.download('punkt')
    nltk.download('wordnet')
    nltk.download('averaged_perceptron_tagger')
    nltk.download('stopwords')

    years = ['2010', '2011', '2012', '2013', '2014', '2015', '2016', '2017', '2018', '2019', '2020']
    restaurants = ['Tibits', 'Mildreds Soho', 'By Chloe']
    for restaurant in restaurants:
        for year in years:
            analyze_restaurant_for_given_year(db_name, restaurant, year)


def analyze_restaurant_for_given_year(db_name, restaurant, year):
    reviews = retrieve_reviews_from_db(db_name, restaurant, year)
    all_reviews = " ".join(reviews)
    words = nltk.word_tokenize(all_reviews)
    stop_words = stopwords.words('english')
    stop_words.append("'s")
    stop_words.append("n't")
    stop_words.append("’")
    normalized_words = remove_noise_and_normalize(words, stop_words)
    freq_dist_pos = nltk.FreqDist(normalized_words)
    fig = plt.figure(figsize=(10, 4))
    freq_dist_pos.plot(10, title=f"Most common words in reviews for {restaurant} in {year}")
    plt.show()
    fig.savefig(f"words_frequency_{restaurant}_{year}.png", bbox_inches="tight")


def remove_noise_and_normalize(words, stop_words):
    cleaned_tokens = []

    for word, tag in pos_tag(words):
        word = re.sub('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+#]|[!*\(\),]|' \
                      '(?:%[0-9a-fA-F][0-9a-fA-F]))+', '', word)

        if tag.startswith("NN"):
            pos = 'n'
        elif tag.startswith('VB'):
            pos = 'v'
        else:
            pos = 'a'

        lemmatizer = nltk.WordNetLemmatizer()
        word = lemmatizer.lemmatize(word, pos)

        if len(word) > 0 and word not in string.punctuation and word.lower() not in stop_words:
            cleaned_tokens.append(word.lower())
    return cleaned_tokens


def retrieve_reviews_from_db(db_file, restaurant_name, year) -> List[str]:
    sql_1 = "SELECT comment "
    sql_2 = "from reviews "
    sql_3 = f"where visit_date BETWEEN '{year}-01-01'and '{year}-12-31'"
    sql_4 = f"and restaurant_name = '{restaurant_name}';"
    connexion = None
    try:
        connexion = sqlite3.connect(db_file)
        c = connexion.cursor()
        sql = sql_1 + sql_2 + sql_3 + sql_4
        c.execute(sql)
        data = c.fetchall()
        reviews = []
        for datum in data:
            reviews.append(datum[0])
        return reviews
    except sqlite3.Error as e:
        print(e)
    finally:
        if connexion:
            connexion.close()


def generate_data_for_histograms(db_name):
    print('histograms...')
    t_histogram = read_db_and_create_histogram(db_name, 'Tibits')
    print('Tibits\n', '\n'.join(t_histogram))
    b_histogram = read_db_and_create_histogram(db_name, 'By Chloe')
    print('By Chloe\n', '\n'.join(b_histogram))
    m_histogram = read_db_and_create_histogram(db_name, 'Mildreds Soho')
    print('Mildreds Soho\n', '\n'.join(m_histogram))


def read_db_and_create_histogram(db_file, restaurant_name):
    sql_1 = "SELECT COUNT(*) "
    sql_2 = "from reviews "

    sql_4 = "and restaurant_name = '" + restaurant_name + "';"
    connexion = None
    dates = [
        "where visit_date BETWEEN '2010-01-01'and '2010-12-31' ",
        "where visit_date BETWEEN '2011-01-01'and '2011-12-31' ",
        "where visit_date BETWEEN '2012-01-01'and '2012-12-31' ",
        "where visit_date BETWEEN '2013-01-01'and '2013-12-31' ",
        "where visit_date BETWEEN '2014-01-01'and '2014-12-31' ",
        "where visit_date BETWEEN '2015-01-01'and '2015-12-31' ",
        "where visit_date BETWEEN '2016-01-01'and '2016-12-31' ",
        "where visit_date BETWEEN '2017-01-01'and '2017-12-31' ",
        "where visit_date BETWEEN '2018-01-01'and '2018-12-31' ",
        "where visit_date BETWEEN '2019-01-01'and '2019-12-31' ",
        "where visit_date BETWEEN '2020-01-01'and '2020-12-31' "
    ]
    try:
        connexion = sqlite3.connect(db_file)
        c = connexion.cursor()
        histogram = []
        for sql_3 in dates:
            sql = sql_1 + sql_2 + sql_3 + sql_4
            c.execute(sql)
            number_of_reviews = c.fetchone()[0]
            histogram.append(str(number_of_reviews))
        connexion.commit()
        return histogram
    except sqlite3.Error as e:
        print(e)
    finally:
        if connexion:
            connexion.close()


if __name__ == '__main__':
    main()
