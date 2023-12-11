import sqlite3
from typing import List

from flair.data import Sentence
from flair.models import TextClassifier


def main():
    db_name = 'restaurants.db'
    years = ['2010', '2011', '2012', '2013', '2014', '2015', '2016', '2017', '2018', '2019', '2020']
    restaurants = ['Tibits', 'Mildreds Soho', 'By Chloe']
    classifier = TextClassifier.load('sentiment')

    for restaurant in restaurants:
        print('restaurant', restaurant)
        for year in years:
            print('year', year)
            sentiment_scores = calcualte_sentiment_for_given_restaurant_year(db_name, classifier, restaurant, year)
            number_of_positive = 0
            number_of_negative = 0
            for score in sentiment_scores:
                if score == 1:
                    number_of_positive = number_of_positive + 1
                else:
                    number_of_negative = number_of_negative + 1
            print('positive', number_of_positive)
            print('negative', number_of_negative * -1)


def calcualte_sentiment_for_given_restaurant_year(db_name, classifier, restaurant, year):
    reviews = retrieve_reviews_from_db(db_name, restaurant, year)
    sentiment_scores = []
    for review in reviews:
        if review == '':
            pass
        else:
            sentence = Sentence(review)
            classifier.predict(sentence)
            sentiment = sentence.get_labels()[0].value
            if sentiment == 'NEGATIVE':
                sentiment_score = -1
            else:
                sentiment_score = 1
            sentiment_scores.append(sentiment_score)
    return sentiment_scores


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


if __name__ == '__main__':
    main()