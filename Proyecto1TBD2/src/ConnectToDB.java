import com.mongodb.BasicDBList;
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
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
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
            document.put("Username","Juana");
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
      BasicDBList dbl = new BasicDBList();
      dbl.add(new BasicDBObject("Adress","0"));
      Document document = new Document("Student", "MongoDB") 
      .append("ID", ID)
      .append("Name", Name) 
      .append("LastName", LastName) 
      .append("PhoneNumber",PhoneNumber) 
      //.append("Adress",Adress) 
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
      collection.updateOne(Filters.eq("ID",ID), Updates.addToSet("Adress","sakjfsakdfsdbfkbsdafksak"));
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
    
    public boolean AssignCarTeacher(String ID, String CarID){
        MongoCollection<Document> collection = database.getCollection("Teacher");
        collection.updateOne(Filters.eq("ID",ID), Updates.set("Assigned Car",CarID));
        HistoryData("Assigned","CarID: "+ID+" to TeacherID: "+ID);
        return true;
    }
    
    public boolean AssignCourseTeacher(String TeacherID, String CourseID){
        MongoCollection<Document> collection = database.getCollection("Course");
        collection.updateOne(Filters.eq("CourseID",CourseID), Updates.set("TeacherID",TeacherID));
        HistoryData("Assigned","TeacherID: "+TeacherID+" to CourseID: "+CourseID);
        return true;
    }
    
    public boolean AssignTestTeacher(String TeacherID, String TestID){
        MongoCollection<Document> collection = database.getCollection("Test");
        collection.updateOne(Filters.eq("TestID",TestID), Updates.set("TeacherID",TeacherID));
        HistoryData("Assigned","TeacherID: "+TeacherID+" to TestID: "+TestID);
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
    
    public DefaultTableModel GetAllCourseDocuments(DefaultTableModel Model){
        Object[] Results=new Object[8];
        MongoCollection<Document> collection = database.getCollection("Course");
        FindIterable<Document> iterDoc=collection.find().projection(Projections.excludeId());
        MongoCursor<Document> it = iterDoc.iterator();
        Document Temp=new Document();
        while(it.hasNext()){
            Temp=it.next();
            Results[0]=Temp.getString("CourseID");
            Results[1]=Temp.getString("Type");
            Results[2]=Temp.getString("Level");
            Results[3]=Temp.getString("Duration");
            Results[4]=Temp.getDouble("Cost");
            Model.addRow(Results);
        }
        return Model;
    }
    
    public DefaultTableModel GetTeacherCourses(DefaultTableModel Model, String teacher_id){
        String[] Results=new String[8];
        MongoCollection<Document> collection = database.getCollection("Course");
        FindIterable<Document> iterDoc=collection.find(Filters.eq("ID", teacher_id)).projection(Projections.excludeId());
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
    
    //JRMS
    public boolean AddCourseDocument(String ID, String Type, String Level, String TeacherID, String Duration, double Cost){
        MongoCollection<Document> collection = database.getCollection("Course");
        Document document = new Document("New Course", "MongoDB") 
        .append("CourseID", ID)
        .append("Level",Level)
        .append("TeacherID", TeacherID)
        .append("Type",Type)
        .append("Duration",Duration)
        .append("Cost",Cost);
        collection.insertOne(document);
        HistoryData("Added","CourseID: "+ID);
        return true;
    }
    
    public boolean ModifyCourseDocument(String ID, String Type, String Level, String Duration, double Cost){
        MongoCollection<Document> collection = database.getCollection("Course");
        collection.updateOne(Filters.eq("CourseID",ID), Updates.set("Type",Type));
        collection.updateOne(Filters.eq("CourseID",ID), Updates.set("Level",Level));
        collection.updateOne(Filters.eq("CourseID",ID), Updates.set("Duration",Duration));
        collection.updateOne(Filters.eq("CourseID",ID), Updates.set("Cost",Cost));
        HistoryData("Modified","CourseID: "+ID);
        return true;
    }
    
    public boolean AddTestDocument(String ID, String Level, String Type, double Cost){
        MongoCollection<Document> collection = database.getCollection("Test");
        Document document = new Document("New Test", "MongoDB") 
        .append("TestID", ID)
        .append("Level",Level)
        .append("Type",Type)
        .append("Cost",Cost);
        collection.insertOne(document);
        HistoryData("Added","TestID: "+ID);
        return true;
    }
    
    public boolean ModifyTestDocument(String ID, String Level, String Type, double Cost){
        MongoCollection<Document> collection = database.getCollection("Test");
        collection.updateOne(Filters.eq("TestID",ID), Updates.set("Type",Type));
        collection.updateOne(Filters.eq("TestID",ID), Updates.set("Level",Level));
        collection.updateOne(Filters.eq("TestID",ID), Updates.set("Cost",Cost));
        HistoryData("Modified","TestID: "+ID);
        return true;
    }
    public boolean DeleteCourseDocument(String ID){
        MongoCollection<Document> collection = database.getCollection("Course");
        collection.deleteOne(Filters.eq("CourseID",ID));
        HistoryData("Deleted","CourseID: "+ID);
        return true;
    }
    
    public boolean DeleteTestDocument(String ID){
        MongoCollection<Document> collection = database.getCollection("Test");
        collection.deleteOne(Filters.eq("ID",ID));
        HistoryData("Deleted","TestID: "+ID);
        return true;
    }
    
    public boolean HistoryData(String Action, String Data){
        MongoCollection<Document> collection = database.getCollection("History");
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date = new Date();
        Document document = new Document("New History", "MongoDB") 
        .append("Action", Action)
        .append("Data", Data)
        .append("Date", dateFormat.format(date)); 
        collection.insertOne(document);
        return true;
    }
    
    public DefaultTableModel HistoryModel(DefaultTableModel Model){
        Object[] Results=new Object[3];
        MongoCollection<Document> collection = database.getCollection("History");
        FindIterable<Document> iterDoc=collection.find().projection(Projections.excludeId());
        MongoCursor<Document> it = iterDoc.iterator();
        Document Temp=new Document();
        while(it.hasNext()){
            Temp=it.next();
            Results[0]=Temp.getString("Action");
            Results[1]=Temp.getString("Data");
            Results[2]=Temp.getString("Date");
            Model.addRow(Results);
        }
        return Model;
    }
} 