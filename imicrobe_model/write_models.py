import argparse
import collections
import io
import os
import pprint
import re

import sqlalchemy as sa


def get_args():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('-o', '--output-fp', default='imicrobe_models.py')

    args = arg_parser.parse_args()
    return args

def main():
    args = get_args()

    ModelWriter().write_models(output_fp=args.output_fp)

class ModelWriter():
    def __init__(self):
        # connect to database on server
        # e.g. mysql+pymysql://imicrobe:<password>@localhost/imicrobe
        db_uri = os.environ.get('IMICROBE_DB_URI')
        self.engine = sa.create_engine(db_uri)
        # reflect tables
        self.meta = sa.MetaData()
        self.meta.reflect(bind=self.engine)

    def get_model_parent_class_name(self):
        return 'Model'

    def import_model_base(self):
        return """\
import sqlalchemy as sa
import sqlalchemy.dialects.mysql as mysql
from sqlalchemy.ext.declarative import declarative_base

Model = declarative_base()

"""

    def get_additional_imports(self):
        return ''

    name_translations = {
        re.compile(r'class'): 'class_'
    }

    def translate_column_name_to_py(self, column_name):
        for pattern, translation in self.name_translations.items():
            if pattern.fullmatch(column_name):
                return pattern.sub(string=column_name, repl=translation)
            else:
                pass
        # if no translation was made return the input unchanged
        return column_name

    type_translations = {
        re.compile(r'BIGINT\(\d+\) UNSIGNED'): r'mysql.BIGINT(unsigned=True)',
        re.compile(r'DATE'): r'sa.Date',
        re.compile(r'DATETIME'): r'sa.DateTime',
        re.compile(r'DOUBLE'): r'sa.Float',
        re.compile(r'ENUM\((.+)\)'): r'mysql.ENUM(\1)',
        re.compile(r'INTEGER\(\d+\)'): r'mysql.INTEGER()',
        re.compile(r'INTEGER\(\d+\) UNSIGNED'): r'mysql.INTEGER(unsigned=True)',
        re.compile(r'LONGTEXT'): r'mysql.LONGTEXT()',
        re.compile(r'MEDIUMTEXT'): r'mysql.MEDIUMTEXT()',
        re.compile(r'TEXT'): r'mysql.TEXT()',
        re.compile(r'TIMESTAMP'): r'mysql.TIMESTAMP',
        re.compile(r'TINYINT\((\d+)\)'): r'mysql.TINYINT(\1)',
        re.compile(r'VARCHAR\((\d+)\)'): r'mysql.VARCHAR(\1)'
    }

    def translate_column_type_to_sa(self, column_type):
        for pattern, translation in self.type_translations.items():
            if pattern.fullmatch(column_type):
                return pattern.sub(string=column_type, repl=translation)
            else:
                pass
        # if no translation was made return the input unchanged
        return column_type

    def write_additional_methods(self, table, table_code):
        return

    def get_many_to_many_relations(self, table):
        insp = sa.engine.reflection.Inspector.from_engine(self.engine)
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
        #pprint.pprint(foreign_key_constraints)
        many_to_many_relations = []
        # TODO: find a better way to identify an association table
        # if this table has exactly 2 foreign keys then it is an association table
        if len(foreign_key_constraints) != 2:
            pass
        else:
            table_0 = self.meta.tables[foreign_key_constraints[0]['referred_table']]
            table_1 = self.meta.tables[foreign_key_constraints[1]['referred_table']]

            table_0_to_table_1 = '{}_to_{}'.format(table_0.name, table_1.name)
            table_1_to_table_0 = '{}_to_{}'.format(table_1.name, table_0.name)
            if table.name == table_0_to_table_1 or table.name == table_1_to_table_0:
                many_to_many_relations.append((table_0, table_1))
            else:
                pass

        return tuple(many_to_many_relations)

    def write_models(self, output_fp):
        # print(meta.tables)

        # build a dictionary of table_name to io.StringIO
        # so we can go back to add relationships
        def string_io_factory():
            return io.StringIO()
        table_to_table_code = collections.defaultdict(string_io_factory)

        insp = sa.engine.reflection.Inspector.from_engine(self.engine)
        for table in self.meta.sorted_tables:
            print(table)

            table_code = table_to_table_code[table]

            # class Table_name(Model):
            table_code.write("class {}({}):\n".format(
                table.name.capitalize(),
                self.get_model_parent_class_name()))

            table_code.write("    __tablename__ = '{}'\n\n".format(table.name))

            # handle foreign keys with explicit ForeignKeyConstraints
            foreign_key_constraints = [
                    "        sa.ForeignKeyConstraint([{}], [{}])".format(
                        ','.join( ["'{}'".format(c_) for c_ in fk_constraint['constrained_columns']] ),
                        ','.join( ["'{}.{}'".format(fk_constraint['referred_table'], r_) for r_ in fk_constraint['referred_columns']] ))
                    for fk_constraint
                    in insp.get_foreign_keys(table_name=table.name)]
            if len(foreign_key_constraints) == 0:
                pass
            else:
                table_code.write('    __table_args__ = (\n')
                table_code.write(',\n'.join(foreign_key_constraints))
                if len(foreign_key_constraints) == 1:
                    # write the trailing ',' for a 1-ple
                    table_code.write(',')
                else:
                    pass
                table_code.write('\n    )\n\n')

            # handle primary key columns
            for pk_column in (table.c[c_] for c_ in insp.get_pk_constraint(table_name=table.name)['constrained_columns']):
                table_code.write(
                    "    {} = sa.Column('{}', {}, primary_key=True)\n".format(
                        self.translate_column_name_to_py(pk_column.name),
                        pk_column.name,
                        self.translate_column_type_to_sa(str(pk_column.type))))

            table_code.write('\n')

            # handle the data columns including foreign key columns
            for column in table.columns:
                if column.primary_key:
                    pass
                else:
                    table_code.write(
                        "    {} = sa.Column('{}', {})  # column.type was '{}'\n".format(
                            self.translate_column_name_to_py(column.name),
                            column.name,
                            self.translate_column_type_to_sa(str(column.type)),
                            column.type))
                    table_code.write('\n')

            table_code.write("\n")

        # add relationships
        relationship_code = """\
    {table_2}_list = sa.orm.relationship(
        "{class_2}",
        secondary="{relationship_table}",
        back_populates="{table_1}_list"
    )

"""
        for table in self.meta.sorted_tables:
            for (table_a, table_b) in self.get_many_to_many_relations(table):
                print('  assume table "{}" represents a many-to-many relationship between tables "{}" and "{}"'.format(table.name, table_a.name, table_b.name))

                table_a_code = table_to_table_code[table_a]
                table_a_code.write(
                    relationship_code.format(
                        table_1=table_a.name,
                        class_2=table_b.name.capitalize(),
                        relationship_table=table.name,
                        table_2=table_b.name))

                table_b_code = table_to_table_code[table_b]
                table_b_code.write(
                    relationship_code.format(
                        table_1=table_b.name,
                        class_2=table_a.name.capitalize(),
                        relationship_table=table.name,
                        table_2=table_a.name))


        for table, table_code in table_to_table_code.items():
            self.write_additional_methods(table, table_code)

        with open(output_fp, 'wt') as test_models:
            test_models.write(self.import_model_base())
            test_models.write(self.get_additional_imports())

            test_models.write(self.get_additional_imports())
            ##flask: test_models.write("from app import db\n\n")
            for _, table_code in sorted(table_to_table_code.items(), key=lambda k_v: k_v[0].name):
                test_models.write(table_code.getvalue())
                test_models.write('\n')


if __name__ == '__main__':
    main()
