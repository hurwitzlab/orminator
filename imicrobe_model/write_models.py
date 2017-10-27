import argparse
import collections
import io
import os
import pprint

import sqlalchemy as sa


def get_args():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('-o', '--output-fp', default='imicrobe_models.py')

    args = arg_parser.parse_args()
    return args


def main():
    args = get_args()

    write_models(output_fp=args.output_fp)


def write_models(output_fp):
    # connect to database on server
    # e.g. mysql+pymysql://imicrobe:<password>@localhost/imicrobe
    db_uri = os.environ.get('IMICROBE_DB_URI')
    imicrobe_engine = sa.create_engine(db_uri)
    # reflect tables
    meta = sa.MetaData()
    meta.reflect(bind=imicrobe_engine)

    # print(meta.tables)

    # build a dictionary of table_name to io.StringIO
    # so we can go back to add relationships
    def string_io_factory():
        return io.StringIO()

    table_name_to_table_code = collections.defaultdict(string_io_factory)

    for table in meta.sorted_tables:
        print(table)

        table_code = table_name_to_table_code[table.name]
        #table_code.write("class {}(db.Model):\n".format(table.name.capitalize()))
        table_code.write("class {}(Model):\n".format(table.name.capitalize()))
        table_code.write("    __tablename__ = '{}'\n".format(table.name))

        for column in table.columns:
            column_name = column.name
            column_attribute_name = column.name
            column_type = str(column.type).split()[0]
            column_modifiers = []

            print('column_name: {}'.format(column_name))
            print('column_type: {}'.format(column_type))

            if column.primary_key:
                column_type = 'Integer'
                column_modifiers.append(', primary_key=True')
            elif column_name.endswith('_id'):
                # assume this column is a foreign key
                column_type = 'Integer'
                column_modifiers.append(
                    ', sa.ForeignKey(\'{}.{}\')'.format(
                        column_name[:-3], column_name))
            elif column.name == 'class':
                column_attribute_name = 'class_'
                column_name = 'class'
            else:
                pass

            # TODO: need different names for model attribute and db column
            # because 'class' is a keyword

            # this is so ugly
            if column_type.startswith('INTEGER'):
                column_type = 'INTEGER'
            elif column_type.startswith('BIGINT'):
                column_type = 'INTEGER'
            elif column_type.startswith('TINYINT'):
                column_type = 'INTEGER'
            elif column_type.startswith('DOUBLE'):
                column_type = 'Float'
            # TODO: what should LONGTEXT map to?
            # TODO: what should MEDIUMTEXT map to?
            elif column_type.startswith('LONGTEXT'):
                column_type = 'Text'
            elif column_type.startswith('MEDIUMTEXT'):
                column_type = 'Text'
            elif column_type.startswith('ENUM'):
                column_type = 'Enum'
            else:
                pass

            table_code.write(
                "    {} = sa.Column('{}', sa.{}{})\n".format(
                    column_attribute_name,
                    column_name,
                    column_type, ''.join(column_modifiers)))

        table_code.write("\n")

        # TODO: make json method optional
        if False:
            table_code.write("    def json(self):\n        return {\n")
            for column in table.columns:
                column_name = column.name
                column_attribute_name = column.name
                if column.primary_key:
                    # do not include primary key in JSON
                    continue
                elif column.name == 'class':
                    column_name = 'class'
                    column_attribute_name = 'class_'
                else:
                    pass

                table_code.write(
                    "            '{}': self.{},\n".format(
                        column_name, column_attribute_name))
            table_code.write("        }")
            table_code.write("\n\n")

        # print(dir(table))
        print(table.columns)

    # add relationships
    relationship_code = """\
    {table_2}_list = sa.orm.relationship(
        "{class_2}",
        secondary="{relationship_table}",
        back_populates="{table_1}_list"
    )

"""
    insp = sa.engine.reflection.Inspector.from_engine(imicrobe_engine)
    for table in meta.sorted_tables:
        foreign_key_constraints = insp.get_foreign_keys(table.name)
        """
        foreign_key_constraints looks like this:
            foreign keys for table sample_to_ontology:
            [{'constrained_columns': ['ontology_id'],
              'name': 'sample_to_ontology_ibfk_2',
              'options': {},
              'referred_columns': ['ontology_id'],
              'referred_schema': None,
              'referred_table': 'ontology'},
             {'constrained_columns': ['sample_id'],
              'name': 'sample_to_ontology_ibfk_3',
              'options': {'ondelete': 'CASCADE'},
              'referred_columns': ['sample_id'],
              'referred_schema': None,
              'referred_table': 'sample'}]
            sample_to_uproc
            foreign keys for table sample_to_uproc:
            [{'constrained_columns': ['sample_id'],
              'name': 'sample_to_uproc_ibfk_1',
              'options': {},
              'referred_columns': ['sample_id'],
              'referred_schema': None,
              'referred_table': 'sample'},
             {'constrained_columns': ['uproc_id'],
              'name': 'sample_to_uproc_ibfk_2',
              'options': {},
              'referred_columns': ['uproc_id'],
              'referred_schema': None,
              'referred_table': 'uproc'}]
        """

        #print(table.name)
        #print('foreign keys for table {}:'.format(table.name))
        #pprint.pprint(insp.get_foreign_keys(table.name))

        # build one-to-one relationships

        # build one-to-many relationships

        # this is still a crappy way to detect many-to-many relationships
        if len(foreign_key_constraints) == 0:
            pass
        elif len(foreign_key_constraints) == 1:
            pass
        elif len(foreign_key_constraints) == 2:
            print('table {} has 2 foreign keys'.format(table.name))
            # if there is a table called 'table_a_to_table_b' or
            # 'table_b_to_table_a' then create a many-to-many relationship
            table_a_name = foreign_key_constraints[0]['referred_table']
            table_b_name = foreign_key_constraints[1]['referred_table']

            association_table_name_ab = '{}_to_{}'.format(table_a_name, table_b_name)
            association_table_name_ba = '{}_to_{}'.format(table_b_name, table_a_name)

            if table.name == association_table_name_ab or table.name == association_table_name_ba:
                print('  assume table "{}" represents a many-to-many relationship between tables "{}" and "{}"'.format(table.name, table_a_name, table_b_name))
                table_a_code = table_name_to_table_code[table_a_name]
                table_a_code.write(
                    relationship_code.format(
                        table_1=table_a_name,
                        class_2=table_b_name.capitalize(),
                        relationship_table=table.name,
                        table_2=table_b_name))

                table_b_code = table_name_to_table_code[table_b_name]
                table_b_code.write(
                    relationship_code.format(
                        table_1=table_b_name,
                        class_2=table_a_name.capitalize(),
                        relationship_table=table.name,
                        table_2=table_a_name))
            else:
                print('  assume table "{}" DOES NOT represent a many-to-many relationship between tables "{}" and "{}"'.format(table.name, table_a_name, table_b_name))

        else:
            pass

    with open(output_fp, 'wt') as test_models:
        test_models.write('import sqlalchemy as sa\n')
        test_models.write('from sqlalchemy.ext.declarative import declarative_base\n')
        test_models.write('\n')

        test_models.write('Model = declarative_base()\n')
        test_models.write('\n')

        ##flask: test_models.write("from app import db\n\n")
        for _, table_code in sorted(table_name_to_table_code.items()):
            test_models.write(table_code.getvalue())
            test_models.write('\n')


if __name__ == '__main__':
    main()
