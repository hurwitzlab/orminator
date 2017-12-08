from collections import defaultdict
from contextlib import contextmanager
import io
import os
import re

from sqlalchemy.orm import sessionmaker
import sqlalchemy as sa


@contextmanager
def session_manager_from_db_uri(db_uri, echo=False):
    """Provide a transactional scope around a series of operations."""
    # connect to database on server
    # e.g. mysql+pymysql://imicrobe:<password>@localhost/muscope2
    session_class = sessionmaker(bind=sa.create_engine(db_uri, echo=echo))
    session = session_class()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


@contextmanager
def session_manager(session_class):
    """Provide a transactional scope around a series of operations."""
    session = session_class()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


class ModelWriter():
    def __init__(self, db_uri):
        # connect to database on server
        # e.g. mysql+pymysql://imicrobe:<password>@localhost/imicrobe
        self.engine = sa.create_engine(db_uri)
        # reflect tables
        self.meta = sa.MetaData()
        self.meta.reflect(bind=self.engine)

    def import_model_base(self):
        return """\
import sqlalchemy as sa
import sqlalchemy.dialects.mysql as mysql
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import backref

Model = declarative_base()

"""

    def get_model_parent_class_name(self):
        return 'Model'

    def get_additional_imports(self):
        return ''

    name_translations = {
        re.compile(r'class'): 'class_',
        re.compile(r'type'): 'type_',
        re.compile(r'file'): 'file_'
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
        re.compile(r'DOUBLE UNSIGNED'): r'sa.Float',
        re.compile(r'ENUM\((.+)\)'): r'mysql.ENUM(\1)',
        re.compile(r'FLOAT'): r'sa.Float',
        re.compile(r'INTEGER\(\d+\)'): r'mysql.INTEGER()',
        re.compile(r'INTEGER\(\d+\) UNSIGNED'): r'mysql.INTEGER(unsigned=True)',
        re.compile(r'LONGTEXT'): r'mysql.LONGTEXT()',
        re.compile(r'MEDIUMTEXT'): r'mysql.MEDIUMTEXT()',
        re.compile(r'TEXT'): r'sa.Text',
        re.compile(r'TIME'): r'sa.Time',
        re.compile(r'TIMESTAMP'): r'mysql.TIMESTAMP',
        re.compile(r'TINYINT\((\d+)\)'): r'mysql.TINYINT(\1)',
        re.compile(r'VARCHAR\((\d+)\)'): r'sa.String(length=\1)'
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
    _association_table_re = re.compile(r'(?P<left_table>.+)_to_(?P<right_table>.+)')
    def get_relations(self, table):
        insp = sa.engine.reflection.Inspector.from_engine(self.engine)

        # if 'table' has a foreign key to a 'table_x' and 'table_x'
        # does not have a foreign key to 'table' then there is a
        # one-to-many relationship between 'table_x' and 'table'
        one_to_many_relations = []

        # TODO: find a better way to identify many-to-many relations
        # if 'table' has 2 foreign keys
        # and has the right name then represents a many-to-many relation
        many_to_many_relations = set()

        for fk_constraint in insp.get_foreign_keys(table.name):
            referred_table = self.meta.tables[fk_constraint['referred_table']]
            referred_table_fk_constraints = insp.get_foreign_keys(referred_table.name)
            referred_table_references_to_table = [
                r_
                for r_
                in referred_table_fk_constraints
                if r_['referred_table'] == table.name]

            if len(referred_table_references_to_table) > 0:
                # 'referred_table' also references 'table'
                # is this a relation?
                print('table "{}" and table "{}" reference each other'.format(
                    table.name, referred_table.name))
            else:
                # 'table' references 'referred_table' but 'referred_table' does
                # not reference 'table'

                # is 'table' a many-to-many association table?
                # if so it should have a name like referred_table_to_another_table
                # or another_table_to_referred_table
                association_table_name_match = self._association_table_re.search(table.name)
                if association_table_name_match:
                    print('table "{}" seems to be a many-to-many relation table'.format(
                        table.name))
                    left_table = self.meta.tables[association_table_name_match.group('left_table')]
                    right_table = self.meta.tables[association_table_name_match.group('right_table')]
                    many_to_many_relations.add((left_table, right_table))
                else:
                    # this looks like a one-to-many relation
                    one_to_many_relations.append({
                        'one': referred_table,
                        'many': table,
                        'fk_constraint': fk_constraint})
        return one_to_many_relations, many_to_many_relations

    def write_models(self, output_fp):
        # print(meta.tables)

        # build a dictionary of table_name to io.StringIO
        # so we can edit the classes repeatedly before writing them to a file
        def string_io_factory():
            return io.StringIO()
        table_to_table_code = defaultdict(string_io_factory)

        insp = sa.engine.reflection.Inspector.from_engine(self.engine)
        for table in self.meta.sorted_tables:
            print(table)

            table_code = table_to_table_code[table]

            # class Table_name(Model):
            table_code.write("class {}({}):\n".format(
                table.name.capitalize(),
                self.get_model_parent_class_name()))

            table_code.write("    __tablename__ = '{}'\n\n".format(table.name))

            # write foreign key constraints using explicit ForeignKeyConstraints
            foreign_key_constraints = []
            for fk_constraint in insp.get_foreign_keys(table_name=table.name):
                constrained_columns_code = ','.join(["'{}'".format(c) for c in fk_constraint['constrained_columns']])
                referred_columns_code = ','.join(["'{}.{}'".format(fk_constraint['referred_table'], r) for r in fk_constraint['referred_columns']])

                all_arguments = ['[{}]'.format(constrained_columns_code), '[{}]'.format(referred_columns_code)]
                for option, value in fk_constraint['options'].items():
                    all_arguments.append("{}='{}'".format(option, value))

                foreign_key_constraints.append("        sa.ForeignKeyConstraint({})".format(','.join(all_arguments)))

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

        # code for one-to-many relations
        one_side_one_to_many_relation_code_template = """\
    {many_table}_list = sa.orm.relationship(
        "{many_class}",
        backref="{one_table}",
        cascade="all, delete-orphan",
        passive_deletes=True)
        #back_populates="{one_table}")

"""
        many_side_one_to_many_relation_cascade_delete_code_template = """\
    #{one_table} = sa.orm.relationship(
        #"{one_class}")
        #backref=backref("{many_table}", passive_deletes=True))
        #back_populates="{many_table}_list")

"""

        many_side_one_to_many_relation_code_template = """\
    #{one_table} = sa.orm.relationship(
        #"{one_class}",
        #back_populates="{many_table}_list")

"""
        # code for many-to-many relations
        relationship_code = """\
    {table_2}_list = sa.orm.relationship(
        "{class_2}",
        secondary="{relation_table}",
        back_populates="{table_1}_list")

"""
        insp = sa.engine.reflection.Inspector.from_engine(self.engine)
        for table in self.meta.sorted_tables:
            one_to_many_relations, many_to_many_relations = self.get_relations(table)
            for one_to_many_relation in one_to_many_relations:
                table_one = one_to_many_relation['one']
                table_many = one_to_many_relation['many']

                # find the fk constraint on the many table referring to the one table
                print('looking for fk constraint on table {}'.format(table_many.name))
                table_many_fk_constraints = insp.get_foreign_keys(table_name=table_many.name)
                many_to_one_fk_constraint = None
                print('looking for foreign key constraint from table_many:"{}" to table_one:"{}"'.format(table_many, table_one))
                for table_many_fk_constraint in table_many_fk_constraints:
                    print(table_many_fk_constraint)
                    if table_one.name in table_many_fk_constraint['referred_table']:
                        many_to_one_fk_constraint = table_many_fk_constraint
                        print('found foreign key constraint {}'.format(table_many_fk_constraint))
                        break
                # did we find it?
                if many_to_one_fk_constraint is None:
                    raise Exception('dammit!')
                elif 'ondelete' in many_to_one_fk_constraint['options']:
                    table_many_code = table_to_table_code[table_many]
                    table_many_code.write(
                        many_side_one_to_many_relation_cascade_delete_code_template.format(
                            one_table=table_one,
                            one_class=table_one.name.capitalize(),
                            many_table=table_many.name))
                else:
                    table_many_code = table_to_table_code[table_many]
                    table_many_code.write(
                        many_side_one_to_many_relation_code_template.format(
                            one_table=table_one,
                            one_class=table_one.name.capitalize(),
                            many_table=table_many.name))

                print('  table "{}" has a one-to-many relationship with table "{}"'.format(
                    table_one, table_many))

                table_one_code = table_to_table_code[table_one]
                table_one_code.write(
                    one_side_one_to_many_relation_code_template.format(
                        many_table=table_many.name,
                        many_class=table_many.name.capitalize(),
                        one_table=table_one.name))

                #table_many_code = table_to_table_code[table_many]
                #table_many_code.write(
                #    many_side_one_to_many_relation_code_template.format(
                #        one_table=table_one,
                #        one_class=table_one.name.capitalize(),
                #        many_table=table_many.name))

            for (table_a, table_b) in many_to_many_relations:
                print('  writing code for many-to-many relation between tables "{}" and "{}"'.format(
                    table_a, table_b))
                table_a_code = table_to_table_code[table_a]
                table_a_code.write(
                    relationship_code.format(
                        table_1=table_a.name,
                        class_2=table_b.name.capitalize(),
                        relation_table=table.name,
                        table_2=table_b.name))

                table_b_code = table_to_table_code[table_b]
                table_b_code.write(
                    relationship_code.format(
                        table_1=table_b.name,
                        class_2=table_a.name.capitalize(),
                        relation_table=table.name,
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
