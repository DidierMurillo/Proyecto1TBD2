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
      //database.createCollection("Student-Class");
    }
    
    public String VerifyUser(String Username,String Password){
        MongoCollection<Document> collection = database.getCollection("User");
        FindIterable<Document> iterDoc=collection.find(Filters.eq("Username",Username)).projection(Projections.excludeId());
        Document document = iterDoc.first();
        if((iterDoc.first( )!=null)){
            if(document.get("Password").equals(Password)){
                //System.out.println(document.getString("ID"));
                document.put("Username","Juana");
                return document.getString("ID");
            }else{
                return "Wrong User";
            }
        } else {
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
    
    //Student
    public boolean AddStudentDocument(String ID,String Name,String LastName,String PhoneNumber,String Adress){
        MongoCollection<Document> collection = database.getCollection("Student");
        FindIterable<Document> iterDoc=collection.find(Filters.eq("ID",ID)).projection(Projections.excludeId());
        if(iterDoc.first() != null){
            return false;
        }
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
        HistoryData("Added","StudentID: "+ID);
        return true;
    }
    
    public boolean ModifyStudentDocument(String ID,String Name,String LastName,String PhoneNumber,String Adress){
        MongoCollection<Document> collection = database.getCollection("Student");
        FindIterable<Document> iterDoc=collection.find(Filters.eq("ID",ID)).projection(Projections.excludeId());
        if(iterDoc.first() == null){
            return false;
        }
        collection.updateOne(Filters.eq("ID",ID), Updates.set("Name",Name));
        collection.updateOne(Filters.eq("ID",ID), Updates.set("LastName",LastName));
        collection.updateOne(Filters.eq("ID",ID), Updates.set("PhoneNumber",PhoneNumber));
        collection.updateOne(Filters.eq("ID",ID), Updates.addToSet("Adress","sakjfsakdfsdbfkbsdafksak"));
        HistoryData("Modified","StudentID: "+ID);
        return true;
    }
    
    public boolean DeleteStudentDocument(String ID){
        MongoCollection<Document> collection = database.getCollection("Student");
        collection.deleteOne(Filters.eq("ID",ID));
        HistoryData("Deleted","StudentID: "+ID);
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
    
    public DefaultTableModel GetStudentCourses(DefaultTableModel Model, String Student_id){
        String[] Results=new String[8];
        String Course_id="";
        MongoCollection<Document> collection = database.getCollection("Student-Class");
        FindIterable<Document> iterDoc=collection.find(Filters.eq("StudentID", Student_id)).projection(Projections.excludeId());
        //MongoCollection<Document> collectionb = database.getCollection("Course");
        MongoCursor<Document> it = iterDoc.iterator();
        Document Temp=new Document();
        while(it.hasNext()){
            Temp=it.next();
            Course_id = Temp.getString("CourseID");
            MongoCollection<Document> collectionb = database.getCollection("Course");
            Results[0] = Course_id;
            Results[1] = GetField("Course","CourseID",Course_id,"Type");
            Results[2] = GetField("Course", "CourseID", Course_id, "Level");
            Results[3] = GetField("Course","CourseID",Course_id,"Duration");
            Results[4] = GetField("Course","CourseID",Course_id,"Cost");
            Results[5] = GetField("Course","CourseID",Course_id,"Status");
            /*Results[0]=Temp.getString("CourseID");
            Results[1]=Temp.getString("Type");
            Results[2]=Temp.getString("Level");
            Results[3]=Temp.getString("Duration");
            Results[4]=Temp.getString("Cost");
            Results[5]=Temp.getString("Status");*/
            Model.addRow(Results);
        }
        return Model;
    }
   
    //Teacher and Assigns
    public boolean AddTeacherDocument(String ID,String Name,String PhoneNumber,String Level){
        MongoCollection<Document> collection = database.getCollection("Teacher");
        FindIterable<Document> iterDoc=collection.find(Filters.eq("TeacherID",ID)).projection(Projections.excludeId());
        if(iterDoc.first() != null){
            return false;
        }
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
        HistoryData("Added","TeacherID: "+ID);
        return true;
    }
    
    public boolean ModifyTeacherDocument(String ID,String Name,String PhoneNumber,String Level){
      MongoCollection<Document> collection = database.getCollection("Teacher");
      FindIterable<Document> iterDoc=collection.find(Filters.eq("TeacherID",ID)).projection(Projections.excludeId());
      if(iterDoc.first() == null){
        return false;
      }
      collection.updateOne(Filters.eq("ID",ID), Updates.set("Name",Name));
      collection.updateOne(Filters.eq("ID",ID), Updates.set("PhoneNumber",PhoneNumber));
      collection.updateOne(Filters.eq("ID",ID), Updates.set("Level",Level));
      HistoryData("Modified","TacherID: "+ID);
      return true;
    }
    
    public boolean AssignCarTeacher(String ID, String CarID){
        MongoCollection<Document> collection = database.getCollection("Teacher");
        MongoCollection<Document> collection2 = database.getCollection("Car");
        FindIterable<Document> iterDoc=collection.find(Filters.eq("TeacherID",ID)).projection(Projections.excludeId());
        if(iterDoc.first() == null){
            return false;
        }
        collection.updateOne(Filters.eq("TeacherID",ID), Updates.set("Assigned Car",CarID));
        collection2.updateOne(Filters.eq("CarID",CarID), Updates.set("TeacherID",ID));
        HistoryData("Assigned","CarID: "+CarID+" to TeacherID: "+ID);
        return true;
    }
    
    public boolean AssignCourseTeacher(String TeacherID, String CourseID){
        MongoCollection<Document> collection = database.getCollection("Course");
        MongoCollection<Document> collection2 = database.getCollection("Teacher");
        FindIterable<Document> iterDoc=collection2.find(Filters.eq("TeacherID",TeacherID)).projection(Projections.excludeId());
        if(iterDoc.first() == null){
            return false;
        }
        collection.updateOne(Filters.eq("CourseID",CourseID), Updates.set("TeacherID",TeacherID));
        HistoryData("Assigned","TeacherID: "+TeacherID+" to CourseID: "+CourseID);
        return true;
    }
    
    public boolean AssignTestTeacher(String TeacherID, String TestID){
        MongoCollection<Document> collection = database.getCollection("Test");
        MongoCollection<Document> collection2 = database.getCollection("Teacher");
        FindIterable<Document> iterDoc=collection2.find(Filters.eq("TeacherID",TeacherID)).projection(Projections.excludeId());
        if(iterDoc.first() == null){
            return false;
        }
        collection.updateOne(Filters.eq("TestID",TestID), Updates.set("TeacherID",TeacherID));
        HistoryData("Assigned","TeacherID: "+TeacherID+" to TestID: "+TestID);
        return true;
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
    
    //Cars
    public boolean AddCarDocument(String ID, String Level, double Km){
        MongoCollection<Document> collection = database.getCollection("Car");
        FindIterable<Document> iterDoc=collection.find(Filters.eq("CarID",ID)).projection(Projections.excludeId());
        if(iterDoc.first() != null){
            return false;
        }
        Document document = new Document("New Car", "MongoDB") 
        .append("CarID", ID)
        .append("Level",Level)
        .append("Kilometers",Km)
        .append("TeacherID","0");
        collection.insertOne(document);
        HistoryData("Added","CarID: "+ID);
        return true;
    }
    
    public boolean ModifyCarDocument(String ID, String Level, double Km){
        MongoCollection<Document> collection = database.getCollection("Car");
        FindIterable<Document> iterDoc=collection.find(Filters.eq("CarID",ID)).projection(Projections.excludeId());
        if(iterDoc.first() == null){
            return false;
        }
        collection.updateOne(Filters.eq("CarID",ID), Updates.set("Level",Level));
        collection.updateOne(Filters.eq("CarID",ID), Updates.set("Kilometers",Km));
        HistoryData("Modified","CarID: "+ID);
        return true;
    }
    
    public boolean DeleteCarDocument(String ID){
        MongoCollection<Document> collection = database.getCollection("Car");
        collection.deleteOne(Filters.eq("CarID",ID));
        HistoryData("Deleted","CarID: "+ID);
        return true;
    }
    
    public DefaultTableModel GetCarDocuments(DefaultTableModel Model){
        Object[] Results=new Object[8];
        MongoCollection<Document> collection = database.getCollection("Car");
        FindIterable<Document> iterDoc=collection.find(Filters.eq("TeacherID","0")).projection(Projections.excludeId());
        MongoCursor<Document> it = iterDoc.iterator();
        Document Temp=new Document();
        while(it.hasNext()){
            Temp=it.next();
            Results[0]=Temp.getString("CarID");
            Results[1]=Temp.getDouble("Kilometers");
            Model.addRow(Results);
        }
        return Model;
    }
    
    public DefaultTableModel GetAllCarDocuments(DefaultTableModel Model){
        Object[] Results=new Object[8];
        MongoCollection<Document> collection = database.getCollection("Car");
        FindIterable<Document> iterDoc=collection.find().projection(Projections.excludeId());
        MongoCursor<Document> it = iterDoc.iterator();
        Document Temp=new Document();
        while(it.hasNext()){
            Temp=it.next();
            Results[0]=Temp.getString("CarID");
            Results[1]=Temp.getString("Level");
            Results[2]=Temp.getDouble("Kilometers");
            Model.addRow(Results);
        }
        return Model;
    }
    
    public boolean AddCourseDocument(String ID, String Type, String Level, String TeacherID, String Duration, double Cost){
        MongoCollection<Document> collection = database.getCollection("Course");
        FindIterable<Document> iterDoc=collection.find(Filters.eq("CourseID",ID)).projection(Projections.excludeId());
        if(iterDoc.first() != null){
            return false;
        }
        Document document = new Document("New Course", "MongoDB") 
        .append("CourseID", ID)
        .append("Level",Level)
        .append("TeacherID", TeacherID)
        .append("Type",Type)
        .append("Duration",Duration)
        .append("Status", "none")
        .append("Cost",Cost);
        collection.insertOne(document);
        HistoryData("Added","CourseID: "+ID);
        return true;
    }
    
    public boolean ModifyCourseDocument(String ID, String Type, String Level, String Duration, double Cost){
        MongoCollection<Document> collection = database.getCollection("Course");
        FindIterable<Document> iterDoc=collection.find(Filters.eq("CourseID",ID)).projection(Projections.excludeId());
        if(iterDoc.first() == null){
            return false;
        }
        collection.updateOne(Filters.eq("CourseID",ID), Updates.set("Type",Type));
        collection.updateOne(Filters.eq("CourseID",ID), Updates.set("Level",Level));
        collection.updateOne(Filters.eq("CourseID",ID), Updates.set("Duration",Duration));
        collection.updateOne(Filters.eq("CourseID",ID), Updates.set("Cost",Cost));
        HistoryData("Modified","CourseID: "+ID);
        return true;
    }
    
    public boolean DeleteCourseDocument(String ID){
        MongoCollection<Document> collection = database.getCollection("Course");
        collection.deleteOne(Filters.eq("CourseID",ID));
        HistoryData("Deleted","CourseID: "+ID);
        return true;
    }
    
    public DefaultTableModel GetCourseDocuments(DefaultTableModel Model, String Course_id){
        Object[] Results=new Object[8];
        MongoCollection<Document> collection = database.getCollection("Course");
        FindIterable<Document> iterDoc=collection.find(Filters.eq("CourseID",Course_id)).projection(Projections.excludeId());
        MongoCursor<Document> it = iterDoc.iterator();
        Document Temp=new Document();
        while(it.hasNext()){
            Temp=it.next();
            Results[0]=Temp.getString("CourseID");
            Results[1]=Temp.getString("Type");
            Results[2]=Temp.getString("Level");
            Results[3]=Temp.getString("Duration");
            Results[4]=Temp.getDouble("Cost");
            Results[5]=Temp.getString("Status");
            Model.addRow(Results);
        }
        return Model;
    }
    
    public DefaultTableModel GetCourseDocuments(DefaultTableModel Model){
        Object[] Results=new Object[8];
        MongoCollection<Document> collection = database.getCollection("Course");
        FindIterable<Document> iterDoc=collection.find(Filters.eq("TeacherID","0")).projection(Projections.excludeId());
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
    
    public boolean ChangeCourseStatus(String ID)
    {
        MongoCollection<Document> collection = database.getCollection("Course");
        if(GetField("Course", "CourseID", ID, "Status").equals("Ongoing"))
        {
            collection.updateOne(Filters.eq("CourseID", ID),Updates.set("Status", "Exams"));
        }
        else if(GetField("Course","CourseID",ID,"Status").equals("Exams"))
        {
            collection.updateOne(Filters.eq("CourseID", ID),Updates.set("Status", "Finalized"));
        }
        else
        {
            return false;
        }
        return true;
    }
    
    //Tests
    public boolean AddTestDocument(String ID, String Level, String Type, double Cost){
        MongoCollection<Document> collection = database.getCollection("Test");
        FindIterable<Document> iterDoc=collection.find(Filters.eq("TestID",ID)).projection(Projections.excludeId());
        if(iterDoc.first() != null){
            return false;
        }
        Document document = new Document("New Test", "MongoDB") 
        .append("TestID", ID)
        .append("Level",Level)
        .append("Type",Type)
        .append("Status","none")
        .append("TeacherID", "0")
        .append("Cost",Cost);
        collection.insertOne(document);
        HistoryData("Added","TestID: "+ID);
        return true;
    }
    
    public boolean ModifyTestDocument(String ID, String Level, String Type, double Cost){
        MongoCollection<Document> collection = database.getCollection("Test");
        FindIterable<Document> iterDoc=collection.find(Filters.eq("TestID",ID)).projection(Projections.excludeId());
        if(iterDoc.first() == null){
            return false;
        }
        collection.updateOne(Filters.eq("TestID",ID), Updates.set("Type",Type));
        collection.updateOne(Filters.eq("TestID",ID), Updates.set("Level",Level));
        collection.updateOne(Filters.eq("TestID",ID), Updates.set("Cost",Cost));
        HistoryData("Modified","TestID: "+ID);
        return true;
    }
    
    public boolean DeleteTestDocument(String ID){
        MongoCollection<Document> collection = database.getCollection("Test");
        collection.deleteOne(Filters.eq("TestID",ID));
        HistoryData("Deleted","TestID: "+ID);
        return true;
    }
    
    public DefaultTableModel GetTestDocuments(DefaultTableModel Model){
        Object[] Results=new Object[8];
        MongoCollection<Document> collection = database.getCollection("Test");
        FindIterable<Document> iterDoc=collection.find(Filters.eq("TeacherID","0")).projection(Projections.excludeId());
        MongoCursor<Document> it = iterDoc.iterator();
        Document Temp=new Document();
        while(it.hasNext()){
            Temp=it.next();
            Results[0]=Temp.getString("TestID");
            Results[1]=Temp.getString("Type");
            Results[2]=Temp.getString("Level");
            Results[3]=Temp.getDouble("Cost");
            Model.addRow(Results);
        }
        return Model;
    }
    
    public DefaultTableModel GetAllTestDocuments(DefaultTableModel Model){
        Object[] Results=new Object[8];
        MongoCollection<Document> collection = database.getCollection("Test");
        FindIterable<Document> iterDoc=collection.find().projection(Projections.excludeId());
        MongoCursor<Document> it = iterDoc.iterator();
        Document Temp=new Document();
        while(it.hasNext()){
            Temp=it.next();
            Results[0]=Temp.getString("TestID");
            Results[1]=Temp.getString("Type");
            Results[2]=Temp.getString("Level");
            Results[3]=Temp.getDouble("Cost");
            Model.addRow(Results);
        }
        return Model;
    }
    
    //History
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
    
    public DefaultTableModel GetLicenseDocuments(DefaultTableModel Model, String IDStudent){
        String[] Results = new String[8];
        MongoCollection<Document> collection = database.getCollection("License");
        FindIterable<Document> iterDoc=collection.find(Filters.eq("ID Student",IDStudent)).projection(Projections.excludeId());
        MongoCursor<Document> it = iterDoc.iterator();
        Document Temp=new Document();
        while(it.hasNext()){
            Temp=it.next();
            Results[0]=Temp.getString("Type");
            Model.addRow(Results);
        }
        return Model;
    }
    
    public DefaultTableModel GetSections(DefaultTableModel Model, String type){
        String[] Results = new String[8];
        MongoCollection<Document> collection = database.getCollection("Course");
        FindIterable<Document> iterDoc=collection.find(Filters.eq("Type",type)).projection(Projections.excludeId());
        MongoCursor<Document> it = iterDoc.iterator();
        Document Temp=new Document();
        while(it.hasNext()){
            Temp=it.next();
            Results[0]=Temp.getString("TeacherID");
            Results[1] = Temp.getString("CourseID");
            Results[2] = Temp.getString("Level");
            Results[3] = Temp.getString("Duration");
            Results[4] = Temp.getDouble("Cost").toString();
            Results[5] = Temp.getString("Level");
            Model.addRow(Results);
        }
        return Model;
    }
    
    public boolean AddStudentCourse(String StudentID, String CourseID){
        MongoCollection<Document> collection = database.getCollection("StudentInClass");
        Document document = new Document("New Student-Class", "MongoDB") 
        .append("ID", StudentID + CourseID)
        .append("StudentID",StudentID)
        .append("CourseID", CourseID);
        collection.insertOne(document);
        return true;
    }
    
    public boolean AddStudentDebit(String StudentID, double amount, String description, String state){
        MongoCollection<Document> collection = database.getCollection("Debit");
        Document document = new Document("New Student-Class", "MongoDB") 
        .append("ID", StudentID + description)
        .append("StudentID", StudentID)
        .append("Amount", amount)
        .append("Description", description)
        .append("State", state);
        collection.insertOne(document);
        HistoryData("Debit Added", "StudentID: " + StudentID + " " + amount + " " + state);
        return true;
    }
    
    public boolean ModifyDebitDocument(String ID, String state){
      MongoCollection<Document> collection = database.getCollection("Student");
      collection.updateOne(Filters.eq("ID",ID), Updates.set("State",state));
      return true;
    }
    
    public boolean GetBoolField(String Collection,String KeyName,String Key,String FieldName){
        MongoCollection<Document> collection = database.getCollection(Collection);
        Document document = collection
            .find(new BasicDBObject(KeyName,Key))
             .projection(Projections.fields(Projections.include(FieldName), Projections.excludeId())).first();
        return document.getBoolean(FieldName);
    }
    
    public double GetDoubleField(String Collection,String KeyName,String Key,String FieldName){
        MongoCollection<Document> collection = database.getCollection(Collection);
        Document document = collection
            .find(new BasicDBObject(KeyName,Key))
             .projection(Projections.fields(Projections.include(FieldName), Projections.excludeId())).first();
        return document.getDouble(FieldName);
    }
} 