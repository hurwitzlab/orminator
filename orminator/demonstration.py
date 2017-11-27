import os

import sqlalchemy as sa

from orminator import session_manager
import orminator.models as models


def main():
    # connect to database on server
    # e.g. mysql+pymysqldb://imicrobe:<password>@localhost/imicrobe
    db_uri = os.environ.get('IMICROBE_DB_URI')
    # set echo=True to send SQL to standard output
    engine = sa.create_engine(db_uri, echo=False)
    # Session is a class
    Session = sa.orm.sessionmaker(bind=engine)

    with session_manager(Session) as session:
        projects = session.query(models.Project).all()
        for project in projects:
            print('\n\n')
            print('Project         : {}'.format(project.project_name))
            print('id              : {}'.format(project.project_id))
            print('investigator(s) :\n\t{}'.format('\n\t'.join([
                investigator.investigator_name for investigator in project.investigator_list])))
            break

    with session_manager(Session) as session:
        sample = models.Sample(file_='/this/is/a/test/sample')
        sample_attr = models.Sample_attr(value='this is a test attribute')
        sample.sample_attr_list.append(sample_attr)

        session.add(sample)

    with session_manager(Session) as session:
        test_sample = session.query(models.Sample).filter(models.Sample.file_ == '/this/is/a/test/sample').one()
        session.delete()



if __name__ == '__main__':
    main()
