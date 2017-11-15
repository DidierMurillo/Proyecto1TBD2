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
      database.createCollection("Professor");
      database.createCollection("License");
      database.createCollection("User");
      database.createCollection("Test");
      database.createCollection("Course");
      database.createCollection("Debit");
      database.createCollection("Car");
      */
    /* 
    //Update a document
    collection.updateOne(Filters.eq("id", 1), Updates.set("likes", 150));       
    System.out.println("Document update successfully...");  
      
    // Deleting the documents 
    collection.deleteOne(Filters.eq("id", 1)); 
    System.out.println("Document deleted successfully..."); 
    */
   }
    
    public boolean VerifyUser(String Username,String Password){
        MongoCollection<Document> collection = database.getCollection("User");
        FindIterable<Document> iterDoc=collection.find(Filters.eq("ID",1)).projection(Projections.excludeId());
        MongoCursor<Document> it = iterDoc.iterator();
        while(it.hasNext()){
            if(it.next().getString("Username").equals(Username)){
                return true;
            }
        }
        return false;
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
      return true;
    }
    
    public boolean ModifyStudentDocument(String ID,String Name,String LastName,String PhoneNumber,String Adress){
      MongoCollection<Document> collection = database.getCollection("Student");
      collection.updateOne(Filters.eq("ID",ID), Updates.set("Name",Name), Updates.set("LastName",LastName));
      collection.updateOne(Filters.eq("ID",ID), Updates.set("PhoneNumber",PhoneNumber), Updates.set("Adress",Adress));
      return true;
    }
    
    public boolean DeleteStudentDocument(String ID){
      MongoCollection<Document> collection = database.getCollection("Student");
      collection.deleteOne(Filters.eq("ID",ID));
      return true;
    }
    
    public DefaultTableModel GetAllDocuments(DefaultTableModel Model){
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
  
} 
      
 

