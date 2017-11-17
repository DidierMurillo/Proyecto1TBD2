import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.client.model.Updates;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import java.util.Iterator;
import javax.swing.table.DefaultTableModel;
import org.bson.Document;

 

public class ConnectToDB {
    MongoClient mongo = new MongoClient("localhost",27017); 
    MongoDatabase database = mongo.getDatabase("Proyecto");
    public void Conection() {  
      // Second Time Document all this 
      /*database.createCollection("Student");
      database.createCollection("Teacher");
      database.createCollection("License");
      database.createCollection("User");
      database.createCollection("Test");
      database.createCollection("Course");
      database.createCollection("Debit");
      database.createCollection("Car");
      */
    }
    
    public String VerifyUser(String Username,String Password){
        MongoCollection<Document> collection = database.getCollection("User");
        FindIterable<Document> iterDoc=collection.find(Filters.eq("Username",Username)).projection(Projections.excludeId());
        Document document = iterDoc.first();
        if(document.get("Password").equals(Password)){
            //System.out.println(document.getString("ID"));
            return document.getString("ID");
        }else{
            return "Wrong User";
        }
        
    }
    
    public String GetField(String Collection,String KeyName,String Key,String FieldName){
        MongoCollection<Document> collection = database.getCollection(Collection);
        Document document = collection
            .find(new BasicDBObject(KeyName,Key))
             .projection(Projections.fields(Projections.include(FieldName), Projections.excludeId())).first();
        return document.getString(FieldName);
    }
    
    //CRUD Student
    public boolean AddStudentDocument(String ID,String Name,String LastName,String PhoneNumber,String Adress){
      MongoCollection<Document> collection = database.getCollection("Student");
      Document document = new Document("Student", "MongoDB") 
      .append("ID", ID)
      .append("Name", Name) 
      .append("LastName", LastName) 
      .append("PhoneNumber",PhoneNumber) 
      .append("Adress",Adress) 
      .append("Pending Debit",0) 
      .append("TeoricalTest", false); 
      collection.insertOne(document);
      //Creating the Student User
      collection = database.getCollection("User");
      document = new Document("New User", "MongoDB") 
      .append("ID",ID)
      .append("Username",ID+Name) 
      .append("Password","123")
      .append("Type","Student"); 
      collection.insertOne(document);
      return true;
    }
    
    public boolean ModifyStudentDocument(String ID,String Name,String LastName,String PhoneNumber,String Adress){
      MongoCollection<Document> collection = database.getCollection("Student");
      collection.updateOne(Filters.eq("ID",ID), Updates.set("Name",Name));
      collection.updateOne(Filters.eq("ID",ID), Updates.set("LastName",LastName));
      collection.updateOne(Filters.eq("ID",ID), Updates.set("PhoneNumber",PhoneNumber));
      collection.updateOne(Filters.eq("ID",ID), Updates.set("Adress",Adress));
      return true;
    }
    
    public boolean DeleteStudentDocument(String ID){
      MongoCollection<Document> collection = database.getCollection("Student");
      collection.deleteOne(Filters.eq("ID",ID));
      return true;
    }
    
    public DefaultTableModel GetAllStudentDocuments(DefaultTableModel Model){
        String[] Results=new String[8];
        MongoCollection<Document> collection = database.getCollection("Student");
        FindIterable<Document> iterDoc=collection.find().projection(Projections.excludeId());
        MongoCursor<Document> it = iterDoc.iterator();
        Document Temp=new Document();
        while(it.hasNext()){
            Temp=it.next();
            Results[0]=Temp.getString("ID");
            Results[1]=Temp.getString("Name");
            Results[2]=Temp.getString("LastName");
            Results[3]=Temp.getString("PhoneNumber");
            Results[4]=Temp.getString("Adress");
            Model.addRow(Results);
        }
        return Model;
    }
    
    //Teacher CRUD
    
    public boolean AddTeacherDocument(String ID,String Name,String PhoneNumber,String Level){
      MongoCollection<Document> collection = database.getCollection("Teacher");
      Document document = new Document("New Teacher", "MongoDB") 
      .append("TeacherID", ID)
      .append("Name", Name) 
      .append("Level",Level) 
      .append("PhoneNumber",PhoneNumber)
      .append("Assigned Car",0); 
      collection.insertOne(document);
      //Creating the Teacher User
      collection = database.getCollection("User");
      document = new Document("New User", "MongoDB") 
      .append("ID",ID)
      .append("Username",ID+Name) 
      .append("Password","1234")
      .append("Type","Teacher"); 
      collection.insertOne(document);
      return true;
    }
    
    public boolean ModifyTeacherDocument(String ID,String Name,String PhoneNumber,String Level){
      MongoCollection<Document> collection = database.getCollection("Student");
      collection.updateOne(Filters.eq("ID",ID), Updates.set("Name",Name));
      collection.updateOne(Filters.eq("ID",ID), Updates.set("PhoneNumber",PhoneNumber));
      collection.updateOne(Filters.eq("ID",ID), Updates.set("Level",Level));
      return true;
    }
    
    //Search NotAssign Methods 
    public DefaultTableModel GetCarDocuments(DefaultTableModel Model){
        String[] Results=new String[8];
        MongoCollection<Document> collection = database.getCollection("Car");
        FindIterable<Document> iterDoc=collection.find(Filters.eq("TeacherID","0")).projection(Projections.excludeId());
        MongoCursor<Document> it = iterDoc.iterator();
        Document Temp=new Document();
        while(it.hasNext()){
            Temp=it.next();
            Results[0]=Temp.getString("CarID");
            Results[1]=Temp.getString("Kilometers");
            Model.addRow(Results);
        }
        return Model;
    }
    
    public DefaultTableModel GetCourseDocuments(DefaultTableModel Model){
        String[] Results=new String[8];
        MongoCollection<Document> collection = database.getCollection("Course");
        FindIterable<Document> iterDoc=collection.find(Filters.eq("Status","Without Teacher")).projection(Projections.excludeId());
        MongoCursor<Document> it = iterDoc.iterator();
        Document Temp=new Document();
        while(it.hasNext()){
            Temp=it.next();
            Results[0]=Temp.getString("CourseID");
            Results[1]=Temp.getString("Type");
            Results[2]=Temp.getString("Level");
            Results[3]=Temp.getString("Duration");
            Model.addRow(Results);
        }
        return Model;
    }
    
    public DefaultTableModel GetTestDocuments(DefaultTableModel Model){
        String[] Results=new String[8];
        MongoCollection<Document> collection = database.getCollection("Test");
        FindIterable<Document> iterDoc=collection.find(Filters.eq("Status","Without Teacher")).projection(Projections.excludeId());
        MongoCursor<Document> it = iterDoc.iterator();
        Document Temp=new Document();
        while(it.hasNext()){
            Temp=it.next();
            Results[0]=Temp.getString("TestID");
            Results[1]=Temp.getString("Type");
            Results[2]=Temp.getString("Level");
            Model.addRow(Results);
        }
        return Model;
    }
    
} 
      
 

