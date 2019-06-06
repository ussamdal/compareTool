package dbCompare;

import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class DataCompare {
	//비교에서 제외할 대상컬럼명을 넣을것
	@SuppressWarnings("serial")
	public static Map<String, String> ignoreColumn = new HashMap<String, String>() {
	{
		put(tidkey,tidkey);
	}};
	public static final String tidkey = "no_tid";
	public static final String newtidkey = "newtid";
	public static final String operateFilePath = "c:\\test\\dbcompare.json";
	public static final String testFilePath = "c:\\test\\dbcompare2.json";
	public static final long time = System.currentTimeMillis(); 
	public static final String stringTime = new SimpleDateFormat("MMddHHmmss").format(time); 
	public static final String outputFilePath = "c:\\test\\result_" + stringTime + ".txt";
	public static void main(String[] args) {
		// TODO Auto-generated method stub
        try {

        	DataCompare dataCompare = new DataCompare();
        	JSONObject operationData = dataCompare.getJsonObject(operateFilePath);
        	JSONObject testData = dataCompare.getJsonObject(testFilePath);
        	dataCompare.compareData(operationData, testData);
        	               
        } catch (Exception e) {
               e.printStackTrace();
        }		
	}

	public JSONObject getJsonObject(String filepath) throws Exception {
		JSONParser parser = new JSONParser();
		Object obj = parser.parse(new FileReader(filepath));
    	return (JSONObject) obj;
	}
	public void addCompareTid (JSONArray jsonArray) throws Exception {
		Iterator iter = jsonArray.iterator();
		while(iter.hasNext()) {
			JSONObject data = (JSONObject) iter.next();
			String tid = (String) data.get(tidkey);
//			System.out.println(tid);
			tid = tid.substring(0, 20) + tid.substring(34, 40);
//			System.out.println(tid);
			data.put(newtidkey, tid);			
		}
		System.out.println(jsonArray);
	}
	public JSONObject findCompareDataByNewTid(String newTid, JSONArray testArray) {
		System.out.println(newTid);
		for(Object testObject : testArray) {
			String testTid = (String)((JSONObject) testObject).get(newtidkey);
			System.out.println("test tid : " + testTid);
			if(testTid.equals(newTid)) {
				System.out.println("Find!! tid : " + testTid);
				return (JSONObject)testObject;
			}
		}
		return null;
	}
	
	public List<Triplet> compareDetailData (JSONObject operationObject, JSONObject testObject) {
		
		List<Triplet> errList = new ArrayList<>();
		JSONObject backupOperation = new JSONObject(operationObject);
		JSONObject backupTest = new JSONObject(testObject);
		for(String key : ignoreColumn.keySet()) {
			backupOperation.remove(key);
			backupTest.remove(key);
		}
		Iterator iterator = backupOperation.keySet().iterator();
		while(iterator.hasNext()) {
			String compareColunm = (String)iterator.next();
			String operationValue = (String)backupOperation.get(compareColunm);
			String testValue = (String)backupTest.get(compareColunm);
			System.out.println(compareColunm + " : " + backupOperation.get(compareColunm));
			System.out.println(compareColunm + " : " + backupTest.get(compareColunm));
			
			
			if(!operationValue.equals(testValue)) {
				errList.add(new Triplet((String) compareColunm, operationValue, testValue));
			}
				
		}
		return errList;
	}
	public void fileWrite (String tid, List<Triplet> errList){
		System.out.println(tid);
		BufferedWriter out = null;
		try {
			out =  new BufferedWriter(new FileWriter(outputFilePath));
			out.write(tid);
			out.newLine();
			for(Triplet te : errList) {
				out.write(te.toString());
				out.newLine();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				if(out != null)
					out.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		
		
	}
	public Boolean compareData (JSONObject operationData, JSONObject testData) throws Exception {
		JSONArray operationArray = (JSONArray) operationData.get("items");
		JSONArray testArray = (JSONArray) testData.get("items");
    	Iterator operationIter = operationArray.iterator();
    	this.addCompareTid(operationArray);
    	this.addCompareTid(testArray);
    	while(operationIter.hasNext()) {
    		JSONObject operationObject = (JSONObject) operationIter.next();
    		System.out.println(operationObject);
    		String tid = (String)operationObject.get(newtidkey);
    		JSONObject testObject = this.findCompareDataByNewTid((String)tid, testArray);
    		List<Triplet> errList = this.compareDetailData(operationObject, testObject); 
    		if(errList.size() == 0) {
    			System.out.println("TID ["+tid+"] Matched!!");
    		} else {
    			this.fileWrite(new String("TID ["+tid+"] is not Matched!!"), errList);
    		}
    	}
		return true;
	}
	
	class Triplet {
		String key;
		String operationData;
		String testData;
		
		public Triplet(String key, String oprationData, String testData) {
			// TODO Auto-generated constructor stub
			this.key = key;
			this.operationData = oprationData;
			this.testData = testData;
		}

		public String getKey() {
			return key;
		}

		public void setKey(String key) {
			this.key = key;
		}

		public String getOperationData() {
			return operationData;
		}

		public void setOperationData(String operationData) {
			this.operationData = operationData;
		}

		public String getTestData() {
			return testData;
		}

		public void setTestData(String testData) {
			this.testData = testData;
		}

		@Override
		public String toString() {
			return "key=[" + key + "], operationData=[" + operationData + "], testData=[" + testData + "]";
		}
		
		
	}
	
}
