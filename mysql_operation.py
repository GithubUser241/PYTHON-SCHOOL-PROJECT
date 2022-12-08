#module_name mysql_operation
'''for doing operation with database by mysql connectivity
It make easier to write query
mysql.connector must be installed
After importing call mysql_operation.connect() and mysql_operation.usedb(database_name) '''
import mysql.connector as sqlcon
datab="new"
host_name='localhost'
user_name='root'
password_use='root'
def connect(host='localhost',user='root',password='root'):
    '''
    connect()
    To connect mysql  
    To change databse use method "usedb " 
    If still error come use mysql.connector.connect() '''
    
    global mycon,cur,datab,host_name,user_name,password_use
    host_name,user_name,password_use=host,user,password
    mycon=sqlcon.connect(host=host,user=user,passwd=password,                           
                         port=3306,auth_plugin='mysql_native_password')
    cur=mycon.cursor()
    
def reconnect():
    '''Reconnect to database'''
    global mycon
    global cur
    if mycon.unread_result:                 #As executing next query can give error due to unread_result
        data=fetchall()
    connect(host_name,user_name,password_use)
    usedb(datab)
    
def disconnect():
    '''To disconnect mysql'''
    if mycon.unread_result:                   #As executing next query can give error due to unread_result
        data=fetchall()
    if mycon.is_connected():                  
        mycon.close()
    
def loop():
    '''Function to use a function of this module more than once'''

    cond=int(input("Enter 1 if you want a finite repetition and 2 for repetition till you no to repetition:- "))
    if cond==1:
        print("Write method_name also the parameter with ()")
        func=input("Enter method name of this module which you want to repeat:- ")
        loop_count=int(input("How many times you want to repeate function:- "))
        for a in range(loop_count):                         #also can call method cursor.executemany()
            eval(func) 
            
    elif cond==2:
        print("Write method_name also the parameter with ()")
        func=input("Enter method name of this module which you want to execute:- ")
        eval(func)
        while True:
            cond2=input("Do you want to use more function or repeat any function:- ")
            if cond2.lower() !="yes":
                break
            cond3=input("If you want to repeat same function write same and if another write another:")
            if cond3.lower()=='same':
                print("I will execute the last function again")
                eval(func)
            else:
                print("Write method_name also the parameter with ()")
                func=input("Write function which you want to execute:-")
                eval(func)
    else:
        print("Wrong input")
        
def query(qry):
    '''query(mysql_query)
    Function to to execute any query of mysql
    ## enter in triple quote for multi line query
    if query give any data then function return a list of data'''
    reconnect()
    cur.execute(qry)
    if mycon.unread_result:                      
        data=cur.fetchall()
        mycon.commit()
        disconnect
        return data
    else:
        mycon.commit() 
        print("Query successfully executed")
        disconnect()
    
def getdb():
    '''To get all databases'''
    connect()
    cur.execute("show databases")
    data=cur.fetchall()
    print("databases:- ")
    
    for rec in data:
        print(rec[0],end=", ")
    print()
    disconnect()

def db_ispresent(db):
    '''
    db_ispresent(database_name) -> Bool
    Function to check database presence'''
    
    connect()
    cur.execute("show databases")
    data=cur.fetchall()
    disconnect()
    cond=False
    for rec in data:
        if db.lower()==rec[0].lower():
            cond=True
            break
    return cond
    disconnect()
        

def create_db(db):
    '''create_db(database_name)
    Function to create database'''

    if db_ispresent(db):
        print("Database already present")
    else:
        connect()
        cur.execute("create database {}".format(db))
        mycon.commit()
        if db_ispresent(db):
            print("Database {} is created".format(db))
        else:
            print("An Error occur \nTry Again")
            
        disconnect()
def drop_db(db):
    '''drop_db(database_name)
    Function to drop database'''
    
    if db_ispresent(db):
        connect()
        cur.execute("drop database {}".format(db))
        mycon.commit()
        if not db_ispresent():
            print("Database {} is drop".format(db))
        else:
            print("An Error occur \nTry Again")
    else:
        print("Database {} is not present".format(db))
        disconnect()
    
def usedb(db):
    '''
    usedb(database_name)
    function to change database'''
    if db_ispresent(db):
        global datab
        datab=db
        connect()
        cur.execute("use {}".format(db))
        mycon.commit()
    else:
        print("Database {} is not present")
        
def gettables():
    '''Show all tables in current database'''
    reconnect()
    cur.execute("show tables")
    data=cur.fetchall()
    print("Tables in database",datab)
    
    for rec in data:
        print(rec[0],end=", ")
    print()
    disconnect()


def ispresent(table):
    '''ispresent(table_name) -> Boolean
    
    Function to check if table is present in database or not'''
    reconnect()
    cur.execute("show tables")
    data=cur.fetchall()
    cond=False
    for rec in data:
        if table.lower()==rec[0].lower():
            cond=True
            break
    disconnect()
    return cond

def create_tb(table):
    '''Function to create table'''
    
    if ispresent(table):
        print("Table already exist \n Use query function to alter table and other work")
        
    else:
        qry_list=["create table {} ( "]
        while True:
            col=input("Enter column name:- ")
            col_type=input("Enter column value type:- ")
            is_constraint=input("Do you want to add column constraint:- ")
            if is_constraint.lower()=="yes":
                print('''Give value for default within ' ' if its type is varchar,char,date,time,etc. not " "
                \nlike default 'Computer' ''')
                col_const=input("Enter column constraint:- ")
            else:
                col_const=" "
            qry_list.extend([col,col_type,col_const,", "])
            cond=input("Do you want to add more column:- ")
            if cond.lower() !="yes":
                break
        qry_list.pop()    # It will remove extra ","
        
        cond2=input("Do you want to add table level constraint:-")
        if cond2.lower()=="yes":
            print("Write full table level constraint \nAlso write all constraint in one go")
            tb_const=input("Enter table level constraint:-")
            qry_list.extend([", ", tb_const," "])
        qry_list.append(" ) ")
        qry=" ".join(qry_list)  
        reconnect()
        
        cur.execute(qry.format(table))
        mycon.commit()
        
        if ispresent(table):
            print("Table creation is successful")
            desc(table)
        else:
            print("An error occur")
        disconnect()
        
def drop_tb(table):
    '''Function to drop table'''
    if ispresent(table):
        reconnect()
        cur.execute("drop table {}".format(table))
        mycon.commit()
        if not ispresent(table):
            print("Table {} is dropped".format(table))
        disconnect()
    else:
        print("Table does not exist")
    
    
def desc(table):
    '''Function to know structure of table'''
    if ispresent(table):
        qry="desc {}".format(table)
        reconnect()
        cur.execute(qry)
        data=cur.fetchall()
        if data==None:
            pass
        else:
            col_name=cur.column_names
            len_list=[]
    
            for col in col_name:
                length=len(col)
                len_list.append(length)
    
            for rec in data:
                for ind in range(len(rec)):
                    if len(str(rec[ind]))>len_list[ind]:
                        len_list[ind]=len(str(rec[ind]))
                
            patternlist=["+"]
            for rec in len_list:
                patternlist.append("-"*rec+"--+")
            pattern="".join(patternlist)
            print(pattern)
    
            data.insert(0,col_name)
            for rec in data:
                for ind in range(len(rec)):
                    differ_size=len_list[ind]-len(str(rec[ind]))
                    blank=(" "*differ_size)+" "
                    print("| ",str(rec[ind]),sep="",end=blank)
                    if ind==len(rec)-1:
                        print("|")
                print(pattern)
        disconnect()

    else:
        print("Table {} is not present in current database".format(table))
        
def getcol(table):
    '''
    getcol(table_name) ->tuple
    #table_name must be str type
    Function to get column_names of table
    return a tuple of column names'''

    if ispresent(table):
        reconnect()
        cur.execute("desc {}".format(table))                  #we can also use cursor.coloumn_names but
        data=cur.fetchall()                                   #we have to execute query on that table for headers
        col_name=[]

        for rec in data:
            col_name.append(rec[0])
        
        col_name=tuple(col_name)
        disconnect()
        return col_name
    else:
        print("Table {} is not present in current database".format(table))

def extract(table):
    '''extract(table_name)
    To get all record of table
    return a list of all records'''
    if ispresent(table):
        reconnect()
        cur.execute("select * from {}".format(table))
        data=cur.fetchall()
        if data==[]:
            print("Empty Set")
        else:
            return data
        disconnect()
    else:
        print("Table {} is not present in current database".format(table))
    
def tabulate_data(data):
    '''tabulate_data(seq)
    Function to tabulate any data
    seq must be tuple or list ,and their element must be tuple or list
    Give column header within a tuple and insert it in the position of first 
    element of seq
    '''
    reconnect()                                 #Here table is made using print, it also have " " (space)
    len_list=[]                                 #for a good table we can use pandas or tabulate module
    if len(data)!=0:
        
        for value in data[0]:
            length=len(value)
            len_list.append(length)
    
        for rec in data:
            for ind in range(len(rec)):
                if len(str(rec[ind]))>len_list[ind]:
                    len_list[ind]=len(str(rec[ind]))
                
        patternlist=["+"]
        for rec in len_list:
            patternlist.append("-"*rec+"--+")
        pattern="".join(patternlist)
        print(pattern)
    
        for ind1 in range(len(data)):
            rec=data[ind1]
            for ind in range(len(rec)):
                differ_size=len_list[ind]-len(str(rec[ind]))
                blank=(" "*differ_size)+" "
                print("| ",str(rec[ind]),sep="",end=blank)
                if ind==len(rec)-1:
                    print("|")
            if ind1==0:
                print(pattern)
        print(pattern)
    disconnect()

def print_table(table):
    '''Function to print all data in table in tabular form like in mysql
    #table_name must be str type'''
    if ispresent(table):
        data1=extract(table)
        if data1!=None and data1!=[]:
            col_name=getcol(table)
            print("Columns in table {} :-".format(table),col_name)
            cond=input("Do you want to print table in an order basis of column:-")
            clause=""
            if cond.lower()=="yes":
                n=int(input("How many column you want to specify order:-"))
                if n>len(col_name):
                    print("Exceeding columns")
                elif n>0:
                    clause="order by "
                    for i in range(n):
                        col=input("Enter column name:-")
                        order=input("Write a for ascending and d for descending:-")
                        clause+=(" "+col+" ")
                        if order.lower()=="d":
                            clause+="desc"
                        else:
                            clause+="asc"       # Ascending will be default if user give any other value
                        if i !=(n-1):
                            clause+=","
            qry="select * from {} {}".format(table,clause)
            data=query(qry)
            col_name=getcol(table)
            data.insert(0,col_name)
            tabulate_data(data)
    else:
        print("Table {} is not present in current database".format(table))
        
def show_record(table):
    '''Function to show all record of table in tuple form'''
    if ispresent(table):
        data=extract(table)
        for rec in data:
            print(rec)
    else:
        print("Table {} is not present in current database".format(table))
        
def insert(table):
    '''Function to insert data in table
    #table_name must be str type
    '''
    if ispresent(table):
        reconnect()
        input_data=[]
        col_name=getcol(table)
        print(col_name)
    
        cond1=input("Do you want all column data to insert:-")
        if cond1.lower()=="yes":
            col_str=" "
        else:
            col_str=input("Enter column names separated by ',' within ( ) bracket:- ")
            col_name=eval(col_str)
            col_str2=""
            for s in col_str:
                if s not in("'",'"'):
                    col_str2=col_str2+s
            col_str=col_str2
            
            
        print("""Enter Name,Date,Time,letter and special charcter with ' ' 
        not " " ""","Enter None for null value",sep="\n")
    
        for col in col_name:
            rec=eval(input("Enter value for {}:-".format(col)))
            input_data.append(rec)
    
        input_data_str=""
        
        
        
        for ind in range(len(input_data)):
            v=input_data[ind]
            if v==None:
                input_data_str+="null"
            else:
                if type(v)==str:
                    input_data_str+=("'"+v+"'")
                else:
                    input_data_str+=str(v)

            if ind!=(len(input_data)-1):    #To insert "," in between value
                input_data_str+=","
                        
        reconnect()
        table=table+col_str
        
        qry="insert into {} values({})".format(table,input_data_str)  
        cur.execute(qry)
        mycon.commit()
        print("Data insertion is completed")
        disconnect()
        
    else:
        print("Table {} is not present in current database".format(table))

def search(table):
    '''Function for searching operation or to show records with less columns
    '''
    #If this function feel big use query() function for searhing (for editor)
    
    reconnect()
    if ispresent(table):
        allcolumn=getcol(table)
        print("Columns in table",table,":-",allcolumn)
        cond1=input("Do you want all column to show:-")
        if cond1.lower()=="yes":
            columns="*"
        else:
            columns=eval(input("Enter column names to show separated by ',' within ( ) bracket:- "))
            str1=",".join(columns)
            columns=str1
    
        cond=int(input("Enter 1 for single value, 2 for range comparision and 3 for all record:-"))
        cond2=True
        
        if cond==1:
            col=input("Enter column name on basis of which searching is to be done without '' :- ")  #searching column
        
            print("""Enter Name,Date,Time,letter and special charcter with ' ' 
            not " " ""","Enter None without ' ' for null value","Enter 'notnull' for not equal to null",sep="\n")
            equalv=eval(input("Enter value for searching:- "))
            if type(equalv)==str:
                if equalv.lower() =="notnull":
                    qry="select {} from {} where {} is not null ".format(columns,table,col)
                
                else:
                    qry="select {} from {} where {} = '{}' ".format(columns,table,col,equalv)
            else:
                if equalv==None:
                    qry="select {} from {} where {} is null".format(columns,table,col)
                else:
                    qry="select {} from {} where {} ={}".format(columns,table,col,equalv)
                    
        elif cond==2:
            col=input("Enter column name on basis of which searching is to be done without '' :- ")  #searching column
            print("""Enter Name,Date,Time,letter and special charcter with ' ' 
            not " " ""","Enter None for null value",sep="\n")            
            lowv=eval(input("Enter lower range value:- "))
            upv=eval(input("Enter upper range value:- "))
            if upv<lowv:
                upv,lowv=lowv,upv  #For extra safety
            if type(lowv)==str:
                qry="select {} from {} where {} between '{}' and '{}' ".format(columns,table,col,lowv,upv)
            else:
                qry="select {} from {} where {} between {} and {}".format(columns,table,col,lowv,upv)
        elif cond==3:
            qry="select {} from {}".format(columns,table)
        else:
            print("Wrong input")
            data=None
            cond2=False
            
        if cond2==True:
            cond4=input("Do you want to print table in an order basis of column:-")
            clause=""
            if cond4.lower()=="yes":
                n=int(input("How many column you want to specify order:-"))
                if n>0:
                    clause="order by "
                for i in range(n):
                    col=input("Enter column name:-")
                    order=input("Write a for ascending and d for descending:-")
                    clause+=(" "+col+" ")
                    if order.lower()=="d":
                        clause+="desc"
                    else:
                        clause+="asc"                 # Ascending will be default if user give any other value
                    if i !=(n-1):
                        clause+=","
            reconnect()
            qry+=(" "+clause)
            cur.execute(qry)
            data=cur.fetchall()
            if data==[]:
                print("Empty Set")
            else:
                print('''Enter 1 if you want to print data in table form \n2 if you want to only to extract data
                \n3 if you want to extract data with column_name''')                  
                cond4=int(input("Enter response"))
                col_name=cur.column_names
                if cond4==1:
                    data.insert(0,col_name)
                    tabulate_data(data)
                elif cond4 ==2:
                    return data
                elif cond4==3:
                    data.insert(0,col_name)
                    return data
                      
        disconnect()
        
    else:
        print("Table {} is not present in current database".format(table))
        
def update(table):
    '''Function to update value in table'''
    col_name=getcol(table)
    print("column in table {} is :-".format(table),col_name)
    n=int(input("How many column value you want to update:- "))
    s1=""
    for i in range(n):
        col1=input("Enter column name whose value is going to change:-")
        print("Give None without ' ' for null,and write charater,date and expression in ' '")
        v1=eval(input("Enter new value:-"))
        s1+=(" "+col1+" = ")
        if v1==None:
            s1+="null"
        elif type(v1)==str:
            s1+=("'"+v1+"'")
        else:
            s1+=str(v1)
        if i !=(n-1):
            s1+=", "
    print("If you do not give where condition all record of the selected columns will be updated")
    cond=input("Do you want to give where condition:-")
    qry="update {} set {}  ".format(table,s1)
    if cond.lower()=="yes":
        print("write : is null for null value")
        clause=input("Enter where clause like where salary<=50000 :- ")
        qry+=(" "+clause)
    reconnect()
    cur.execute(qry)
    mycon.commit()                                         #commit is use to ensure that query implement on database
    print("Updation successful")
    disconnect()

def delete(table):
    ''' Function to delete records from a table'''
    cond=input("Do you want to delete all records from table {} :-".format(table))
    if cond.lower()=="yes":
        qry="delete from {}".format(table)
        reconnect()
        cur.execute(qry)                                     #Also can use query() function of this module
        mycon.commit()
        print("All records deleted")
        disconnect()
    else:
        print("write : is null for null value")
        clause=input("Write where clause like where Book_name is null:- ")
        qry="delete from {} {}".format(table,clause)
        reconnect()
        cur.execute(qry)                                        #Also can use query() function of this module
        mycon.commit()                                          #commit is use to ensure that query implement on database
        print("Done")
        disconnect()
        
        
        
        

                
            
