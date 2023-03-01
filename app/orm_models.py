from datetime import datetime, date

from sqlalchemy import create_engine, Column, Integer, String, Date, DateTime, Float, Boolean, Index, ForeignKey
from sqlalchemy.exc import NoResultFound
from sqlalchemy.orm import sessionmaker, declarative_base, relationship, backref
from sqlalchemy.ext.hybrid import hybrid_method
from sqlalchemy.ext.associationproxy import association_proxy

engine = create_engine("sqlite:///orm_python.db", echo=True) # echo=True - чтобы в консоль писались все запросы
Session = sessionmaker(bind=engine)
session = Session()

Base = declarative_base(bind=engine)


class Author(Base):
    __tablename__ = 'authors'
    __table_args__ = (Index('id_author_index', 'id'),)

    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    surname = Column(String(100), nullable=False)

    def __repr__(self):
        return f"{self.name}, {self.surname}"

    @classmethod
    def get_all_authors(cls):
        return session.query(Author).all()

    def to_json(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


class Book(Base):
    __tablename__ = 'books'
    __table_args__ = (Index('id_book_index', 'id'),)

    id = Column(Integer, primary_key=True)
    name = Column(String(16), nullable=False)
    count = Column(Integer, default=1)
    release_date = Column(Date, nullable=False)
    author_id = Column(Integer, ForeignKey('authors.id'), nullable=False)

    author = relationship("Author", backref=backref("books",
                                                    cascade="all, "
                                                            "delete-orphan",
                                                    lazy="select"))

    students = relationship("Receiving", back_populates="book")

    def __repr__(self):
        return f"{self.name}, {self.count}, {self.release_date}, {self.author_id}"

    @classmethod
    def get_all_books(cls):
        return session.query(Book).all()

    def to_json(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


class Student(Base):
    __tablename__ = 'students'
    __table_args__ = (Index('id_student_index', 'id'),)

    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    surname = Column(String(100), nullable=False)
    phone = Column(String(11), nullable=False)
    email = Column(String(100), nullable=False)
    average_score = Column(Float, nullable=False)
    scholarship = Column(Boolean, nullable=False)

    books = relationship("Receiving", back_populates="student")

    associations = association_proxy("books", "receiving_books")

    def __repr__(self):
        return f"{self.name}, {self.surname}, {self.phone}, {self.email}, {self.average_score}, {self.scholarship}"

    @classmethod
    def get_all_students(cls):
        return session.query(Student).all()

    @classmethod
    def get_scholarship_students(cls):
        return session.query(Student).filter(cls.scholarship is not None).all()

    @classmethod
    def get_students_by_score(cls, income_score: float):
        try:
            students = session.query(Student.id).filter(cls.average_score > income_score).subquery()
            return students
        except NoResultFound:
            print('No students found')

    def to_json(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


class Receiving(Base):
    __tablename__ = 'receiving_books'

    book_id = Column(Integer, ForeignKey('books.id'), primary_key=True)
    student_id = Column(Integer, ForeignKey('students.id'), primary_key=True)

    date_of_issue = Column(DateTime, default=datetime.now)
    date_of_return = Column(DateTime)

    student = relationship("Student", back_populates="books")
    book = relationship("Book", back_populates="students")

    def __repr__(self):
        return f"{self.book_id}, {self.student_id}, {self.date_of_issue}, {self.date_of_return}"

    @classmethod
    def get_all_receiving(cls):
        return session.query(Receiving).all()

    @hybrid_method
    def is_debtors(self, compare_date):
        return self.date_of_issue < compare_date

    # @classmethod
    # def get_stundets_too_much_days(cls, num_of_days: int):
    #     return session.query(cls.student_id).filter(count_date_with_book > num_of_days).all()

    def to_json(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


def insert_data():
    authors = [Author(name="Alexander", surname="Pushkin"),
               Author(name="Lev", surname="Tolstoy"),
               Author(name="Mihail", surname="Bulgakov"),
               ]
    authors[0].books.extend([Book(name="Captain daughter",
                                  count=5,
                                  release_date=date(1836, 1, 1)),
                             Book(name="Evgenii Onegin",
                                  count=3,
                                  release_date=date(1838, 1, 1))
                             ])
    authors[1].books.extend([Book(name="War And Peace",
                                  count=10,
                                  release_date=date(1867, 1, 1)),
                             Book(name="Anna Karenina",
                                  count=7,
                                  release_date=date(1877, 1, 1))
                             ])
    authors[2].books.extend([Book(name="Morfiy",
                                  count=5,
                                  release_date=date(1926, 1, 1)),
                             Book(name="Dog's heart",
                                  count=3,
                                  release_date=date(1925, 1, 1))
                             ])
    students = [Student(name="Nik", surname="Nokiv", phone="2", email="3",
                        average_score=4.5, scholarship=True),
                Student(name="Vlad", surname="Filatov", phone="87", email="4",
                        average_score=4, scholarship=True)]
    session.add_all(authors)
    session.add_all(students)
    session.commit()


def give_me_book():
    nikita = session.query(Student).filter(Student.name == 'Nik').one()
    vlad = session.query(Student).filter(Student.name == 'Vlad').one()
    books_to_nik = session.query(Book).filter(Author.surname == 'Tolstoy',
                                              Author.id == Book.author_id).all()
    books_to_vlad = session.query(Book).filter(Book.id.in_([1, 3, 4])).all()

    for book in books_to_nik:
        receiving_book = Receiving()
        receiving_book.book = book
        receiving_book.student = nikita
        session.add(receiving_book)

    for book in books_to_vlad:
        receiving_book = Receiving()
        receiving_book.book = book
        receiving_book.student = vlad
        session.add(receiving_book)

    session.commit()


if __name__ == '__main__':

    Base.metadata.create_all(bind=engine)
    check_exist = session.query(Author).all()
    if not check_exist:
        insert_data()
        give_me_book()

