import os

import sqlalchemy as sa

import imicrobe_model.models as models


def main():
    # connect to database on server
    # e.g. mysql+pymysqldb://imicrobe:<password>@localhost/imicrobe
    db_uri = os.environ.get('IMICROBE_DB_URI')
    # set echo=True to send SQL to standard output
    engine = sa.create_engine(db_uri, echo=False)
    # Session is a class
    Session = sa.orm.sessionmaker(bind=engine)

    session = Session()

    projects = session.query(models.Project).all()
    for project in projects:
        print('\n\n')
        print('Project         : {}'.format(project.project_name))
        print('id              : {}'.format(project.project_id))
        print('investigator(s) :\n\t{}'.format('\n\t'.join([
            investigator.investigator_name for investigator in project.investigator_list
        ])))


if __name__ == '__main__':
    main()
