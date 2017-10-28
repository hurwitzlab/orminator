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

    def get_additional_imports(self):
        return ''

    def find_fk_constraint_by_column_name(self, column_names, fk_constraints):
        """
        Given a list of column names and a list of foreign key constaint
        dictionaries, return a list of foreign key constaint dictionaries
        corresponding to the column names (in the same order). If a column
        name does not have a foreign key constraint return None in its place.
        """
        found_fk_contraints = []
        # return fk_constraints in the same order as column_names
        for column_name in column_names:
            found_fk_contraint = None
            for fk_constraint in fk_constraints:
                # TODO: what if more than one constrained column?
                if len(fk_constraint['constrained_columns']) > 1:
                    raise Exception()
                elif fk_constraint['constrained_columns'][0] == column_name:
                    found_fk_contraint = fk_constraint
                    break
                else:
                    # keep looking
                    pass
            # found_fk_contraint may be None and that is ok
            found_fk_contraints.append(found_fk_contraint)

        return tuple(found_fk_contraints)

    def write_json_method(self):
        return False

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
        # find two-column unique constraints in which both columns
        # are foreign keys -- this criterion may be too strict
        """
        many_to_many_relations = []
        for unique_constraint in insp.get_unique_constraints(table.name):
            if len(unique_constraint['column_names']) == 2:
                column_name_a, column_name_b = unique_constraint['column_names']
                fk_constraint_a, fk_constraint_b = self.find_fk_constraint_by_column_name(
                    (column_name_a, column_name_b), foreign_key_constraints)
                if fk_constraint_a is None:
                    pass
                elif fk_constraint_b is None:
                    pass
                else:
                    # both members of the unique constraint are also
                    # foreign keys
                    many_to_many_relations.append(
                        (table.c[column_name_a], table.c[column_name_b]))
        """
        print('')
        print(table.name)
        pprint.pprint(foreign_key_constraints)
        many_to_many_relations = []
        if len(foreign_key_constraints) != 2:
            pass
        else:
            table_0 = self.meta.tables[foreign_key_constraints[0]['referred_table']]
            table_1 = self.meta.tables[foreign_key_constraints[1]['referred_table']]
            many_to_many_relations.append((table_0, table_1))

        return tuple(many_to_many_relations)

    def write_models(self, output_fp):
        # print(meta.tables)

        # build a dictionary of table_name to io.StringIO
        # so we can go back to add relationships
        def string_io_factory():
            return io.StringIO()

        table_name_to_table_code = collections.defaultdict(string_io_factory)

        for table in self.meta.sorted_tables:
            print(table)

            table_code = table_name_to_table_code[table.name]
            #table_code.write("class {}(db.Model):\n".format(table.name.capitalize()))

            # class Table_name(Model):
            table_code.write("class {}({}):\n".format(
                table.name.capitalize(),
                self.get_model_parent_class_name()))

            table_code.write("    __tablename__ = '{}'\n".format(table.name))

            for column in table.columns:
                print('table {} has column {} with type {}'.format(
                    table.name, column.name, column.type))

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
            if self.write_json_method():
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
        for table in self.meta.sorted_tables:
            for (table_a, table_b) in self.get_many_to_many_relations(table):
                print('  assume table "{}" represents a many-to-many relationship between tables "{}" and "{}"'.format(table.name, table_a.name, table_b.name))

                table_a_code = table_name_to_table_code[table_a.name]
                table_a_code.write(
                    relationship_code.format(
                        table_1=table_a.name,
                        class_2=table_b.name.capitalize(),
                        relationship_table=table.name,
                        table_2=table_b.name))

                table_b_code = table_name_to_table_code[table_b.name]
                table_b_code.write(
                    relationship_code.format(
                        table_1=table_b.name,
                        class_2=table_a.name.capitalize(),
                        relationship_table=table.name,
                        table_2=table_a.name))


        with open(output_fp, 'wt') as test_models:
            test_models.write('import sqlalchemy as sa\n')
            test_models.write('from sqlalchemy.ext.declarative import declarative_base\n')
            test_models.write('\n')

            test_models.write('Model = declarative_base()\n')
            test_models.write('\n')

            test_models.write(self.get_additional_imports())
            ##flask: test_models.write("from app import db\n\n")
            for _, table_code in sorted(table_name_to_table_code.items()):
                test_models.write(table_code.getvalue())
                test_models.write('\n')


if __name__ == '__main__':
    main()
