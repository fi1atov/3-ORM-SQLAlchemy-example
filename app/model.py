import os

from flask import Flask, request, jsonify
from datetime import datetime
from datetime import timedelta
import csv

from sqlalchemy import func

from orm_models import Base, session, Book, Receiving, Author, Student

app = Flask(__name__)


@app.before_request
def before_request_func():
    Base.metadata.create_all()


@app.route('/')
def hello_world():
    return 'Hello World!', 200


@app.route('/get_all_books', methods=["GET"])
def get_all_books():
    """
        получить все книги
    """
    books = session.query(Book).all()
    books_list = []
    for book in books:
        books_list.append(book.to_json())
    return jsonify(books_list=books_list), 200


@app.route('/get_book_in_lib_by_author/<int:author_id>', methods=["GET"])
def get_book_in_lib_by_author(author_id: int):
    """
        получить кол-во оставшихся в библиотеке книг по автору
        (GET -входной параметр - ID автора)
    """
    books = session.query(Book).filter(Book.count != 0, Book.author_id == author_id)
    books_list = []
    for book in books:
        books_list.append(book.to_json())
    return jsonify(books_list=books_list), 200


@app.route('/get_not_read_books/<int:student_id>', methods=["GET"])
def get_not_read_books(student_id: int):
    """
        получить список книг, которые студент не читал,
        при этом другие книги этого автора студент уже брал
        (GET - входной параметр - ID студента).
    """
    readed_books = session.query(Receiving.book_id)\
        .filter(Receiving.student_id == student_id).subquery()
    readed_authors = session.query(Book.author_id)\
        .filter(Book.id.in_(readed_books))

    books = session.query(Book)\
        .filter(Book.id.not_in(readed_books),
                Book.author_id.in_(readed_authors))

    books_list = []
    for book in books:
        books_list.append(book.to_json())
    return jsonify(books_list=books_list), 200


@app.route('/get_avg_taken_books', methods=["GET"])
def get_avg_taken_books():
    """
        получить среднее кол-во книг, которые студенты брали в этом месяце (GET)
    """
    avg_num = session.query(func.avg(Receiving.book_id))\
        .filter(func.strftime('%Y-%m', Receiving.date_of_issue) == func.strftime('%Y-%m', 'now'))
    val = avg_num[0][0]
    return jsonify(avg_books=val), 200


@app.route('/get_students_over_fourteen_days', methods=["GET"])
def get_students_over_fourteen_days():
    """
        получение список должников, которые держат книги у себя более 14 дней. (GET)
    """
    deadline = datetime.now() - timedelta(days=14)
    bad_students = session.query(Receiving.student_id).filter(Receiving.is_debtors(deadline)).all()

    students_list = []
    for student in bad_students:
        students_list.append(student[0])
    return jsonify(students_list=students_list), 200


@app.route('/get_popular_book_high_score', methods=["GET"])
def get_popular_book_high_score():
    """
        получить самую популярную книгу среди студентов,
        у которых средний балл больше 4.0 (GET)
    """
    # 1. Получить студентов у которых средний балл выше 4
    students = Student.get_students_by_score(4.0)

    # 2. Получить сколько книг читает каждый студент.
    # Отсортировать по убыванию и взять первую строку чтобы
    # определить идентификатор самой популярной книги
    book_id = session.query(
        Receiving.book_id)\
        .filter(Receiving.student_id.in_(students))\
        .group_by(Receiving.book_id)\
        .order_by(func.count(Receiving.book_id).desc()).first()

    book = session.query(Book).filter(Book.id == book_id[0])

    return jsonify(books_list=book[0].to_json()), 200


@app.route('/get_top_reading_students', methods=["GET"])
def get_top_reading_students():
    """
        получить ТОП-10 самых читающих студентов в этом году (GET)
        (в итоге получаю 3 студента, потому что у меня их мало - но суть решения)
    """
    students = session.query(Student)\
        .join(Receiving)\
        .group_by(Student.id)\
        .order_by(func.count(Receiving.book_id).desc()).limit(3)

    students_list = []
    for student in students:
        students_list.append(student.to_json())
    return jsonify(students_list=students_list), 200


@app.route('/load_students_csv', methods=["POST"])
def load_students_csv():
    """
        роут, который принимает csv-файл с данными по студентам (разделитель ;)
    """
    data = []
    if request.files:
        uploaded_file = request.files['students_file']
        if not uploaded_file:
            return "No file"

        filepath = os.path.join(os.path.abspath(uploaded_file.filename))
        uploaded_file.save(filepath)

        with open(filepath, newline='') as csvfile:
            reader = csv.DictReader(csvfile, delimiter=';')
            for row in reader:
                # data.append(row) # так просто не получилось, потому что значения поля scholarship не
                # воспринимались как логические. Приходится использовать bool
                data.append(
                    {
                        'name': row["name"],
                        'surname': row["surname"],
                        'phone': row["phone"],
                        'email': row["email"],
                        'average_score': row["average_score"],
                        'scholarship': bool(row["scholarship"])
                     }
                )

        session.bulk_insert_mappings(Student, data)
        session.commit()

    return 'Информация по студентам загружена', 201


@app.route('/give_book_to_student', methods=["POST"])
def give_book_to_student():
    """
        студент берет книгу
    """
    book_id = request.form.get('book_id', type=int)
    student_id = request.form.get('student_id', type=int)
    current_datetime = datetime.now()

    new_receiving = Receiving(book_id=book_id, student_id=student_id, date_of_issue=current_datetime)
    session.add(new_receiving)
    session.commit()

    return 'Студенту дали книгу', 201


@app.route('/receiving_book', methods=["POST"])
def receiving_book():
    """
        студент возвращает книгу
    """
    book_id = request.form.get('book_id', type=int)
    student_id = request.form.get('student_id', type=int)
    current_datetime = datetime.now()

    receiving = session.query(Receiving).filter(
        Receiving.book_id == book_id,
        Receiving.student_id == student_id).one_or_none()

    receiving.date_of_return = current_datetime
    session.commit()

    return 'Студент вернул книгу в библиотеку', 200


if __name__ == '__main__':
    app.run()
