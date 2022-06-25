import pandas as pd
import sys
from datetime import datetime
from datetime import timedelta
import pymysql
from sqlalchemy import create_engine
import mysql.connector

def main():
        db=mysql.connector.connect( host="localhost", user='root', password="its sameer17", database="practice")
        mycursor=db.cursor(buffered=True , dictionary=True) 

        query=""" select customerID as StudentID, Name as StudentName,department as Department,email
                as Mail,city as City,country as Country,marks as TotalMarks,payment as feesPaid from
                practice.customers inner join practice.department on customerID=empID inner join 
                practice.emails on customerID=emailID inner join practice.marks on customerID=marksID
                inner join practice.payments on customerID=paymentID inner join
                practice.contacts on customerID=contactID; """

        result_practice=mycursor.execute(query)
        result_practice=mycursor.fetchall()
        dataF=pd.DataFrame(result_practice)
        print(dataF)


        try:
            my_conn = create_engine("mysql+mysqldb://root:its sameer17@localhost/practice")   
            dataF.to_sql(con=my_conn,name="student",if_exists='append',index=False)
        except:
            print("already exists")

        studentID=int(input("enter the studentID= "))
        mydata=(studentID,)
        output_query="""select StudentName, Mail,Department,TotalMarks, feesPaid from practice.student where StudentID=%s"""
        ouput=mycursor.execute(output_query,mydata)
        output=mycursor.fetchall()
        dataF=pd.DataFrame(output)
        print("**********************************************************************************************************")
        print(dataF)
        print("**********************************************************************************************************")
        
        amount=input("y/n= ")
        mydata1=(studentID,)
        if amount=='y':
            amount_paid=int(input("amount_paid=  "))
            k=1000-amount_paid
            mydata1=(k,studentID,)
            amount_query="""UPDATE practice.student SET feesPaid = %s WHERE (StudentID = %s);  """
            output_paid=mycursor.execute(amount_query,mydata1)
            output_paid=mycursor.fetchall()
            dataF_paid=pd.DataFrame(output_paid)
            print("**********************************************************************************************************")
            print(dataF_paid)
        else:
            print("due will be charged") 


if __name__ == "__main__":
    print ("Executed when invoked directly")
    main()

