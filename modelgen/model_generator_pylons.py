from sqlalchemy import *
from sqlalchemy.exc import SAWarning
import re
#from sqlalchemy.databases.mysql import *
from sqlalchemy.dialects.mysql.base import *
#from sqlalchemy.dialects.mssql.base import *

'''
SQL ALCHEMY BASIC MODEL GENERATOR V0.1
@author: eric li(modifier)
'''


class Params(object):
    ''' all settings class '''

    #DB CONNECTION
    #dialect to specify. should work for postgres sqlite and mysql
    dialect = ''

    #db user name to connect
    user = ''

    #password to connect to database
    password = ''

    #host to connect 
    host = ''

    #database name to connect
    db_name = ''

    #Relative path needed for sqlite
    sqlite_abs_path = None
    
    #schema to connecto to if this
    schema = None

    # list of tables to autofetch the information for
    table_list = []

    #Specify table class name if  this is None it uses CameCase notation (eg for user_password it Creates UserPassword)
    class_name = None

    #set the metadata associated with this colmn set
    metadata_name = 'Not defined metadata'

    #specify the backref names if None it will uses generated #TODO:fix generation of backrefs
    fk_backref_name = None

    #generate the imports at beginning generate string with all needed imports in order to work with this model
    #generally you should leave this to True if you generating model only set it to false.
    with_imports = True

    #force to have primary keys
    force_pk = True


    @staticmethod
    def fetch_all_tables_pg(cls):
        con_str = '%s://%s:%s@%s/%s' \
        % (cls.dialect, cls.user, cls.password, cls.host, cls.db_name)
        engine = create_engine(con_str)

        conn = engine.connect()
        res = conn.execute("""
                                show tables;""")
        lst = []
        for row in res:
            lst.append(str(row[0]))
        conn.close()
        return lst


class BaseModelGenerator(object):

    def __init__(self, params):


        self.engine = create_engine(self.get_conn_str(params))
        self.temp_meta = MetaData(bind=self.engine)
        self.params = params
        self.imports = """%s\n%s\n%s\n%s\n%s\n%s\n\n"""\
        % ('from sqlalchemy.ext.declarative import declarative_base',
            'from sqlalchemy.orm import relation, backref',
            'from sqlalchemy import ForeignKey, Column, Table, Sequence',
            'from sqlalchemy.types import *',
            self.get_dialect_import(params),
            'from put_your_project_here.model.meta import Base')


        #self.class_name_def = """class %s(Base): """
        self.def_string = '    sa.Column("%(c_name)s", %(type)s, nullable=%(nullable)s,unique=%(uniq)s,default=%(default)s),'
        self.def_string_pk1 = '    sa.Column("%(c_name)s", %(type)s, nullable=%(nullable)s,unique=%(uniq)s,default=%(default)s, primary_key = %(primary_key)s),'
        self.def_string_pk2 = '    sa.Column("%(c_name)s", %(type)s, sa.Sequence("%(table_name)s_seq_%(c_name)s", optional=True), nullable=%(nullable)s,unique=%(uniq)s,default=%(default)s, primary_key = %(primary_key)s),'
        self.def_string_fk = '    %(name)s = Column("%(c_name)s", %(type)s,%(fk_col)s,nullable=%(nullable)s,unique=%(uniq)s,default=%(default)s),'
        self.def_string_pk_fk = '    %(name)s = Column("%(c_name)s", %(type)s,%(fk_col)s,nullable=%(nullable)s,unique=%(uniq)s,default=%(default)s,primary_key = %(primary_key)s),'
        self.fk_set = []


    def get_conn_str(self, params):
        dialects = ['mysql', 'sqlite', 'postgres']
        if params.dialect not in dialects:
            raise RuntimeError('inproper dialect use one of %s' % dialects)
        if params.dialect == 'sqlite':
            if not params.sqlite_abs_path:
                raise RuntimeError('for sqlite you have to set params.sqlite_abs_path')
            
            conn_str = '%s:///%s/%s' \
            % (params.dialect, params.sqlite_abs_path, params.db_name)
        else:       
            conn_str = '%s://%s:%s@%s/%s' \
            % (params.dialect, params.user, params.password,
               params.host, params.db_name)        
        return conn_str
    
    def get_dialect_import(self, params):
        if params.dialect == 'mysql':
            return 'from sqlalchemy.databases.mysql import *'
        if params.dialect == 'postgres':
            return 'from sqlalchemy.databases.postgres import *'
        if params.dialect == 'sqlite':
            return 'from sqlalchemy.databases.sqlite import *'
    
    def get_class_name(self, table_name):
        if self.params.class_name is None:
            #pattern to replace _ and - to ' ' in order to make CamelCase
            tmp_class = table_name.replace("_", ' ').replace("-", ' ')
            return self.camelcase(tmp_class)
        else:
            return table_name

    def my_capitalize(self, value):
        ''' capitalize string '''
        return str(value[0].upper()) + "".join([w for w in str(value)[1:]])

    def camelcase(self, value):
        ''' Make camelcase out of table_names '''
        return "".join([self.my_capitalize(w) for w in re.split(re.compile("[\W_]*"), value)])

    def fk_insert(self, fk_orderedSet):
        return ','.join([str(x) for x in fk_orderedSet])

    def auto_fetch_table(self, table_name):
        try:
            tbl = Table(table_name, self.temp_meta, autoload=True, schema=self.params.schema)
        except (SAWarning, Warning) as warn:
            pass
        return tbl

    def check_for_pk(self, table_obj):
        pk_exist = False

        for table in table_obj.columns:
            pk_exist = table.primary_key != False
            if pk_exist:
                break

        if pk_exist is False and self.params.force_pk is True:
            str = '[%s] table is missing a PRIMARY KEY fix that error your model will not work with table without PK' % table_obj.name
            raise Exception(str)

    def fk_defines(self, column, table_obj):


        #TODO: ADD FUNC for more than one FK
        fk = column.foreign_keys[0]._colspec.split('.')

        if self.params.schema == None or self.params.schema == '':
            col_desc_proper = fk[0]
            fk_backref_name = "%s_on_%s" % (table_obj.name , col_desc_proper)
        else:
            col_desc_proper = fk[1]
            fk_backref_name = "%s_on_%s" % (table_obj.name , col_desc_proper)

        #TODO...check
        if self.params.schema:
            pj = 'primaryjoin = "%s.%s==%s.%s"' % (self.camelcase(table_obj.name), column.name, self.camelcase(fk[1]) , fk[2])
        else:
            pj = 'primaryjoin = "%s.%s==%s.%s"' % (self.camelcase(table_obj.name), column.name, self.camelcase(fk[0]) , fk[1])

        rel = """    %s = relation('%s', backref='%s',%s)""" % \
        (table_obj.name + '_' + col_desc_proper + '_rel', self.camelcase(col_desc_proper), fk_backref_name, pj)

        self.fk_set.append(rel)

    def generate_table_string(self, table_name):
        ''' Generate table = sa.Table string for table_name
        
        :param table_name: table name to generate
        
        '''
        auto_table = self.auto_fetch_table(table_name)
        #check if table has a PK if not this function will raise an exception
        self.check_for_pk(auto_table)

        print "%s_table = sa.Table('%s', meta.metadata," % (table_name, table_name)
        for i in auto_table.columns:
            # mapping type to SQLAlchemy types
            coltype = i.type
            if isinstance(coltype, MSInteger):
                coltype = "types.Integer()" 
            elif isinstance(coltype, MSString):
                coltype = "types.Unicode(%s)" %  i.type.length
            elif isinstance(coltype, MSText):
                coltype = "types.UnicodeText()"
            elif isinstance(coltype, MSDecimal):
                coltype = "types.Numeric(precision=%s, scale=%s)" % (i.type.precision, i.type.scale)
            elif isinstance(coltype, MSTime):
                coltype = "types.TIMESTAMP()"
            #elif isinstance(coltype, MSDateTime):
            #    coltype = "types.DateTime()"
            elif isinstance(coltype, MSTimeStamp):
                coltype = "types.DateTime()"
            #elif isinstance(coltype, MSDate):
            #    coltype = "types.DateTime()"
            #elif isinstance(coltype, MSBoolean):
            #    coltype = "types.Boolean()"

            if i.primary_key and not i.foreign_keys:
                if i.name == 'id':
                    print self.def_string_pk2 % {'c_name':i.name,
                                           'type':coltype,
                                           'table_name':table_name,
                                           'nullable':i.nullable,
                                           'uniq':i.unique,
                                           'default':i.default,
                                           'primary_key':i.primary_key}
                else:
                    print self.def_string_pk1 % {'c_name':i.name,
                                           'type':coltype,
                                           'nullable':i.nullable,
                                           'uniq':i.unique,
                                           'default':i.default,
                                           'primary_key':i.primary_key}

            elif i.foreign_keys and not i.primary_key:
                print self.def_string_fk % {'name':i.name,
                                    'c_name':i.name,
                                    'type':coltype,
                                    'nullable':i.nullable,
                                    'uniq':i.unique,
                                    'default':i.default,
                                    'fk_col':self.fk_insert(i.foreign_keys)}
                self.fk_defines(i, auto_table)

            elif i.foreign_keys and i.primary_key:
                print self.def_string_pk_fk % {'name':i.name,
                                    'c_name':i.name,
                                    'type':coltype,
                                    'nullable':i.nullable,
                                    'uniq':i.unique,
                                    'default':i.default,
                                    'primary_key':i.primary_key,
                                    'fk_col':self.fk_insert(i.foreign_keys)}
                self.fk_defines(i, auto_table)

            else:
                print self.def_string % {'c_name':i.name,
                                    'type':coltype,
                                    'nullable':i.nullable,
                                    'uniq':i.unique,
                                    'default':i.default}

        if auto_table._foreign_keys:
            print '\n    #definition of foreignkeys backrefs'
            for f in self.fk_set:
                print f
        print ")"
        
    def generate_class_string(self, table_name):
        print "class %s(MetaObject):" % self.get_class_name(table_name)
        print "    pass"

    def generate_orm_string(self, table_name):
        print "orm.mapper(%s, %s_table)" % (self.get_class_name(table_name), table_name)
        
    def generate_model(self):
        if self.params.with_imports:
            print self.imports

        for table_name in self.params.table_list:
            self.fk_set = []
            self.generate_table_string(table_name)
            print ""
        print ""

        for table_name in self.params.table_list:
            self.generate_class_string(table_name)
            print ""
        print ""

        for table_name in self.params.table_list:
            self.generate_orm_string(table_name)
