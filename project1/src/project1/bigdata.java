package project1;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;


public class bigdata {
	

		
	static HashMap<String, rgb> colormap = new HashMap<String, rgb>();
	static HashMap<String, Integer> sizemap = new HashMap<String, Integer>();

	public static void main(String[] args) throws IOException {
		try {
			color_map();
			size_map();
			similarity();
			
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// similarity();
	}

	public static void similarity() throws Exception {
		HashMap<String, Double> similarity = new HashMap<String, Double>();

		String csvFile = "C:/Users/Arvin/Desktop/dhyitha1.csv";
		String csvFile1 = "C:/Users/Arvin/Desktop/dhyitha1.csv";
		BufferedReader br = null;
		BufferedReader br1 = null;
		String line = "";
		String cvsSplitBy = ",";
		String item = null;
		Integer lcnt = 0;
		Integer lcnt2 = 0;
		String diff = null;
		String color = null;
		String size = null;
		String dept = null;
		String clas = null;
		String subclass = null;
		String seasonid = null;

		String item1 = null;

		String diff1 = null;
		String color1 = null;
		String sizc1 = null;
		String dept1 = null;
		String clas1 = null;
		String subclass1 = null;
		String seasonid1 = null;

		Integer count = 0;
		Integer count1 = 0;

		rgb rgbval = new rgb();

		br = new BufferedReader(new FileReader(csvFile));
		while ((line = br.readLine()) != null  ) {
			//System.out.println(line);
			lcnt++;
			count = 0;
			System.out.println(lcnt);
			for (String retval : line.split(",")) {

				//System.out.println(retval);
				count++;
				// str =retval ;
				if (count == 1) {
					item = retval;
				}
				if (count == 2) {
					color = retval;
				}
				if (count == 3) {
					size = retval;
				}
				if (count == 4) {
					dept = retval;
				}
				if (count == 5) {
					clas = retval;
				}
				if (count == 6) {
					subclass = retval;
				}
				if (count == 7) {
					seasonid = retval;
				}
			}
			br1 = new BufferedReader(new FileReader(csvFile1));
			while ((line = br1.readLine()) != null) {
				//lcnt2++;
				count1 = 0;
				for (String retval : line.split(",")) {

					//System.out.println(retval);
					count1++;
					// str =retval ;
					if (count1 == 1) {
						item1 = retval;
					}
					if (count1 == 2) {
						color1 = retval;
					}
					if (count1 == 3) {
						sizc1 = retval;
					}
					if (count1 == 4) {
						dept1 = retval;
					}
					if (count1 == 5) {
						clas1 = retval;
					}
					if (count1 == 6) {
						subclass1 = retval;
					}
					if (count1 == 7) {
						seasonid1 = retval;
					}
				}
				Double simclr = color_calc(color, color1);
				
				String flag = null;
				String flag1 = null;
				try {
					Integer dummy = Integer.parseInt(size);
					flag = "Y";
				} catch (NumberFormatException e) {
					//System.out.println("Wrong number");
					flag = "N";
				}
				try {
					Integer dummy = Integer.parseInt(sizc1);
					flag1 = "Y";
				} catch (NumberFormatException e) {
					//System.out.println("Wrong number");
					flag1 = "N";
				}

				Double simsize = size_calc(size,flag, sizc1,flag1);
				int simcls =0;
				int simsbcls =0;
				int simdept = rest_of_feilds(dept, dept1);
				if (simdept!=0){
				   simcls = rest_of_feilds(clas, clas1);
				  if (simcls!=0){
					  simsbcls = rest_of_feilds(subclass, subclass1);  
				  }
				}
				
				
				int simsbssn = rest_of_feilds(seasonid, seasonid1);

				Double simtot = simclr + simsize + simdept + simcls + simsbcls
						+ simsbssn;
  
				String key_patrn = item + "-" + item1;
				/*System.out.println("simclr"+simclr);
				System.out.println("simsize"+simsize);
				System.out.println("simdept"+simdept);
				System.out.println("simcls"+simcls);
				System.out.println("simsbcls"+simsbcls);
				System.out.println("simsbssn"+simsbssn);*/
				if (simtot > 11){
				 similarity.put(key_patrn, simtot);
				}
			}
		}
		
		System.out.println("simsize"+similarity.size());
		Set<Entry<String, Double>> set2 = similarity.entrySet(); 
		Iterator iterator2 = set2.iterator(); 
		while(iterator2.hasNext()) { 
		 Map.Entry
		 mentry2 = (Map.Entry)iterator2.next();
		 System.out.print("Key is: "+mentry2.getKey() + " & Value is: ");
		 System.out.println(mentry2.getValue()); }
	
	}

	public static void size_map() {
		

		sizemap.put("0/S",0);
		sizemap.put("L",2);
		sizemap.put("L/XL",3);
		sizemap.put("M",7);
		sizemap.put("M/L",9);
		sizemap.put("S",4);
		sizemap.put("S/M",5);
		sizemap.put("XL",38);
		sizemap.put("XS",1);
		sizemap.put("XS/S",1);
		sizemap.put("XXL",44);
		sizemap.put("XXS",0);
		sizemap.put("N/A",0);
		
		
	}
	public static Double size_calc(String s1,String f1, String s2 ,String f2){
		/*System.out.println("str1"+s1);
		System.out.println("flg1"+f1);
		System.out.println("str2"+s2);
		System.out.println("flg2"+f2);*/
		Integer val1 = 0 ;
		Integer val2 = 0 ;
		Double Mean = 20.28571;
		if (Objects.equals(f1, "N")){
			val1 =  sizemap.get(s1);
		}else{
			val1 = Integer.parseInt(s1);
		}
		if(Objects.equals(f2, "N")){
			val2 =  sizemap.get(s2);
		}else{
			val2 = Integer.parseInt(s2);
		}
		Double val1d = val1/Mean;
		//System.out.println("val1"+val1d);
		Double val2d = val2/Mean;
		//System.out.println("val2"+val2d);
		//System.out.println("res"+ Math.abs(val1-val2));
		return 3 - (double) Math.abs(val1d-val2d);
	}

	public static int rest_of_feilds(String s1, String s2) {
		if (Objects.equals(s1, s2)) {
			return 3 ;
		}
		return 0;
	}

	public static void color_map() throws Exception {
		String csvFile = "C:/Users/Arvin/Desktop/color1.csv";
		BufferedReader br = null;
		String line = "";
		Integer count = 0;
		rgb rgbval = new rgb();
		String str = null;
		int rd = 0;
		int bl = 0;
		int gr = 0;

		br = new BufferedReader(new FileReader(csvFile));
		while ((line = br.readLine()) != null) {

			System.out.println(line);
			// String[] newline=line.split(cvsSplitBy);
			count = 0;
			for (String retval : line.split(",")) {

				//System.out.println(retval);
				count++;
				// str =retval ;
				if (count == 1) {
					str = retval;
				}
				if (count == 2) {
					str.valueOf(rd);
				}
				if (count == 3) {
					str.valueOf(bl);
				}
				if (count == 4) {
					str.valueOf(gr);
				}
			}
			rgbval.R = rd;
			rgbval.G = bl;
			rgbval.B = gr;
			colormap.put(str, rgbval);
		}
		// System.out.println("Map key and values after removal:");
		System.out.println("clrsize"+colormap.size());
		br.close();
		/*
		* Set<Entry<String, rgb>> set2 = colormap.entrySet(); Iterator
		* iterator2 = set2.iterator(); while(iterator2.hasNext()) { Map.Entry
		* mentry2 = (Map.Entry)iterator2.next();
		* System.out.print("Key is: "+mentry2.getKey() + " & Value is: ");
		* System.out.println(mentry2.getValue()); }
		*/
	}
	
	public static double color_calc(String c1, String c2) throws Exception {
		 rgb rgbval1 = new rgb();
		 rgb rgbval2 = new rgb();
		 if (!colormap.containsKey(c1)){
			 rgbval1.R = 50 ;
			 rgbval1.G = 50 ;
			 rgbval1.B = 50 ;
		 }else{
			 rgbval1 =colormap.get(c1); 
		 }
		 if (!colormap.containsKey(c2)){
			 rgbval2.R = 50 ;
			 rgbval2.G = 50 ;
			 rgbval2.B = 50 ;
		 }else{
			 rgbval2 =colormap.get(c2); 
		 }
		 
		 long rmean = ( (long)rgbval1.R + (long)rgbval2.R ) / 2;
		 long r = (long)rgbval1.R - (long)rgbval2.R;
		 long g = (long)rgbval1.G - (long)rgbval2.G;
		 long b = (long)rgbval1.B - (long)rgbval2.B;
		 return 3 - Math.sqrt((((512+rmean)*r*r)>>8) + 4*g*g + (((767-rmean)*b*b)>>8))/100;
	}

}