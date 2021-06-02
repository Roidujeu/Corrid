package org.Roidujeu.corrid;

import java.io.IOException;
import java.util.Scanner;
import java.io.FileReader;
import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;
import java.lang.Math;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Corrid extends Configured implements Tool {
	static ArrayList<String> listOfCountries = new ArrayList<String>();
	static ArrayList<String> listOfCountriesII = new ArrayList<String>();
	static ArrayList<String> listOfCountriesIII = new ArrayList<String>();
	static String date_copy = "";
	static int daysToPredict = -9999;
	static ArrayList<Double> listOfX_es = new ArrayList<Double>();
	static ArrayList<Double> listOfTot_Vacc_es = new ArrayList<Double>();
	static ArrayList<Double> listOfDaily_Vacc_es = new ArrayList<Double>();
/***
 * Map Class I
 * Correlation Module
 ***/
	public static class MapClass extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

		private Text word = new Text();
		private Text values = new Text();

		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			String line = value.toString();
			StringTokenizer tokens = new StringTokenizer(line," ");
			int counter = 0;
			String key_out = null;
			String totDeathString = null, popDenString = null, ageString = null, gdpString = null;
			String cardioVascString = null, tbString = null, pneuString = null, bronchString = null,emphyString = null;
			String asthmaString = null, diabString = null, lifeExpString = null, hdiString = null;
			int numOfCountries = listOfCountries.size();
			loop:while (tokens.hasMoreTokens() && counter<19) {
				String str = tokens.nextToken();
				switch (counter) {
					case 2: {
						boolean countryAdded = false;
						if(numOfCountries == 0) {
							listOfCountries.add(str);	
						} else {
							for(int g=0; g<numOfCountries; g++) {
								if(str.equals(listOfCountries.get(g))) {
									countryAdded = true;
									break;
								}
							}
						}
						if(countryAdded == false) {
							listOfCountries.add(str);
						}
						key_out = listOfCountries.get((listOfCountries.size()==0?0:listOfCountries.size()-1));
						break;
					}
				
					case 4: {
						totDeathString = str;
						break;
					}
					case 7: {
						popDenString = str;
						break;
					}
					case 8: {
						ageString = str;
						break;
					}
					case 9: {
						gdpString = str;	
						break;
					}
					case 10: {
						cardioVascString = str;
						break;
					}
					case 11: {
						tbString = str;
						break;
					}
					case 12: {
						pneuString = str;
						break;
					}
					case 13: {
						bronchString = str;
						break;
					}
					case 14: {
						emphyString = str;
						break;
					}
					case 15: {
						asthmaString = str;
						break;
					}
					case 16: {
						diabString = str;
						break;
					}
					case 17: {
						lifeExpString = str;
						break;
					}
					case 18: {
						hdiString = str;
						break;
					}
					default: {
						break;
					}
				}
				counter++;
			}
			String value_str = (totDeathString.concat(" ".concat(popDenString))).concat(" ".concat(ageString)).concat(" ".concat(gdpString.concat(" ".concat(cardioVascString)))).concat(" ".concat(tbString)).concat(" ".concat(pneuString.concat(" ".concat(bronchString)))).concat(" ".concat(emphyString)).concat(" ".concat(asthmaString.concat(" ".concat(diabString)))).concat(" ".concat(lifeExpString)).concat(" ".concat(hdiString));
			word.set(key_out);
			values.set(value_str);
			output.collect(word, values);
		}
	}
	
/***
 * Reduce Class I
 * Correlation Module
 ***/

	public static class ReduceClass extends MapReduceBase
			implements Reducer<Text, Text, Text, Text> {
		private Text value_out_text = new Text();
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			String totDeathString = null, popDenString = null, ageString = null, gdpString = null;
			String cardioVascString = null, tbString = null, pneuString = null, bronchString = null,emphyString = null;
			String asthmaString = null, diabString = null, lifeExpString = null, hdiString = null;
			int count = 0; int secCount = 0;
			double sumOfDeaths = 0, sumOfDens = 0, sumOfAge = 0, sumOfGDP = 0, sumOfCardioVasc = 0, sumOfTB = 0, sumOfPneu = 0, sumOfBronch = 0, sumOfEmphy = 0, sumOfAsthma = 0, sumOfDiab = 0, sumOfLifeExp = 0, sumOfHDI = 0;
			String avgOfDeaths = null, avgOfDens = null, avgOfAge = null, avgOfGDP = null, avgOfCardioVasc = null, avgOfTB = null, avgOfPneu = null, avgOfBronch = null, avgOfEmphy = null, avgOfAsthma = null, avgOfDiab = null, avgOfLifeExp = null, avgOfHDI = null;
			
			while (values.hasNext()) {
				secCount = 0;
				String str = values.next().toString();
				StringTokenizer breakdown = new StringTokenizer(str," ");
				while(breakdown.hasMoreTokens() && secCount < 13) {
					String ind = breakdown.nextToken();
					switch(secCount) {
						case 0: {
							totDeathString = ind;
							break;
						}
						case 1: {
							popDenString = ind;
							break;
						}
						case 2: {
							ageString = ind;
							break;
						}
						case 3: {
							gdpString = ind;	
							break;
						}
						case 4: {
							cardioVascString = ind;
						}
						case 5: {
							tbString = ind;
							break;
						}
						case 6: {
							pneuString = ind;
							break;
						}
						case 7: {
							bronchString = ind;
							break;
						}
						case 8: {
							emphyString = ind;
							break;
						}
						case 9: {
							asthmaString = ind;
							break;
						}
						case 10: {
							diabString = ind;
							break;
						}
						case 11: {
							lifeExpString = ind;
							break;
						}
						case 12: {
							hdiString = ind;
							break;
						}
						default: {
							break;
						}
					}
					secCount++;
				}
				count++;
				sumOfDeaths += Double.valueOf(totDeathString);
				sumOfDens += Double.valueOf(popDenString);
				sumOfAge += Double.valueOf(ageString);
				sumOfGDP += Double.valueOf(gdpString);
				sumOfCardioVasc += Double.valueOf(cardioVascString);
				sumOfTB += Double.valueOf(tbString);
				sumOfPneu+= Double.valueOf(pneuString);
				sumOfBronch+= Double.valueOf(bronchString);
				sumOfEmphy += Double.valueOf(emphyString);
				sumOfAsthma+= Double.valueOf(asthmaString);
				sumOfDiab += Double.valueOf(diabString);
				sumOfLifeExp+= Double.valueOf(lifeExpString);
				sumOfHDI+= Double.valueOf(hdiString);
			}
			
			avgOfDeaths = String.valueOf(sumOfDeaths/count);
			avgOfDens = String.valueOf(sumOfDens/count);
			avgOfAge = String.valueOf(sumOfAge/count);
			avgOfGDP = String.valueOf(sumOfGDP/count);
			avgOfCardioVasc = String.valueOf(sumOfCardioVasc/count);
			avgOfTB = String.valueOf(sumOfTB/count);
			avgOfPneu= String.valueOf(sumOfPneu/count);
			avgOfBronch= String.valueOf(sumOfBronch/count);
			avgOfEmphy = String.valueOf(sumOfEmphy/count);
			avgOfAsthma= String.valueOf(sumOfAsthma/count);
			avgOfDiab = String.valueOf(sumOfDiab/count);
			avgOfLifeExp= String.valueOf(sumOfLifeExp/count);
			avgOfHDI= String.valueOf(sumOfHDI/count);
		

			String value_str = (avgOfDeaths).concat(" ".concat(avgOfDens)).concat(" ".concat(avgOfAge)).concat(" ".concat(avgOfGDP)).concat(" ".concat(avgOfCardioVasc)).concat(" ".concat(avgOfTB)).concat(" ".concat(avgOfPneu)).concat(" ".concat(avgOfBronch)).concat(" ".concat(avgOfEmphy)).concat(" ".concat(avgOfAsthma)).concat(" ".concat(avgOfDiab)).concat(" ".concat(avgOfLifeExp)).concat(" ".concat(avgOfHDI));
			
			value_out_text.set(value_str); 
			output.collect(key, value_out_text);
		}
	}

/***
 * Map Class II
 * Correlation Module
 ***/
	
	public static class MapClass2 extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

		private Text word = new Text();
		private Text values = new Text();

		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			String line[] = value.toString().split("\\s+");
			int counter = 0;
			String key_out = "";
			String totDeathString = null, popDenString = null, ageString = null, gdpString = null;
			String cardioVascString = null, tbString = null, pneuString = null, bronchString = null,emphyString = null;
			String asthmaString = null, diabString = null, lifeExpString = null, hdiString = null;
			loop:while (counter<14) {
				String str = line[counter];
				switch (counter) {	
					case 1: {
						totDeathString = str;
						break;
					}
					case 2: {
						popDenString = str;
						break;
					}
					case 3: {
						ageString = str;
						break;
					}
					case 4: {
						gdpString = str;	
						break;
					}
					case 5: {
						cardioVascString = str;
						break;
					}
					case 6: {
						tbString = str;
						break;
					}
					case 7: {
						pneuString = str;
						break;
					}
					case 8: {
						bronchString = str;
						break;
					}
					case 9: {
						emphyString = str;
						break;
					}
					case 10: {
						asthmaString = str;
						break;
					}
					case 11: {
						diabString = str;
						break;
					}
					case 12: {
						lifeExpString = str;
						break;
					}
					case 13: {
						hdiString = str;
						break;
					}
					default: {
						break;
					}
				}
				counter++;
			}
			String value_str = (totDeathString.concat(" ".concat(popDenString))).concat(" ".concat(ageString)).concat(" ".concat(gdpString.concat(" ".concat(cardioVascString)))).concat(" ".concat(tbString)).concat(" ".concat(pneuString.concat(" ".concat(bronchString)))).concat(" ".concat(emphyString)).concat(" ".concat(asthmaString.concat(" ".concat(diabString)))).concat(" ".concat(lifeExpString)).concat(" ".concat(hdiString));
			word.set(key_out);
			values.set(value_str);
			output.collect(word, values);
		}
	}
	
/***
 * Reduce Class II
 * Correlation Module
 ***/

	public static class ReduceClass2 extends MapReduceBase
			implements Reducer<Text, Text, Text, Text> {
		private Text value_out_text = new Text();
		private Text key_out_text = new Text();
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			int count = 0; int secCount = 0;
			double totDeathsD = 0, popDensD = 0, ageD = 0, gdpD = 0, cardioVascD = 0, tbD = 0, pneuD = 0, bronchD = 0, emphyD = 0, asthmaD = 0, diabD = 0, lifeExpD = 0, hdiD = 0;
			double sumOfDeaths = 0, sumOfDens = 0, sumOfAge = 0, sumOfGDP = 0, sumOfCardioVasc = 0, sumOfTB = 0, sumOfPneu = 0, sumOfBronch = 0, sumOfEmphy = 0, sumOfAsthma = 0, sumOfDiab = 0, sumOfLifeExp = 0, sumOfHDI = 0;
			double sqSumOfDeaths = 0, sqSumOfDens = 0, sqSumOfAge = 0, sqSumOfGDP = 0, sqSumOfCardioVasc = 0, sqSumOfTB = 0, sqSumOfPneu = 0, sqSumOfBronch = 0, sqSumOfEmphy = 0, sqSumOfAsthma = 0, sqSumOfDiab = 0, sqSumOfLifeExp = 0, sqSumOfHDI = 0;
			double sumSqOfDeaths = 0, sumSqOfDens = 0, sumSqOfAge = 0, sumSqOfGDP = 0, sumSqOfCardioVasc = 0, sumSqOfTB = 0, sumSqOfPneu = 0, sumSqOfBronch = 0, sumSqOfEmphy = 0, sumSqOfAsthma = 0, sumSqOfDiab = 0, sumSqOfLifeExp = 0, sumSqOfHDI = 0;
			double deathDens = 0, deathAge = 0, deathGDP = 0, deathCardioVasc = 0, deathTB = 0, deathPneu = 0, deathBronch = 0, deathEmphy = 0, deathAsthma = 0, deathDiab = 0, deathLifeExp = 0, deathHDI = 0;
			double sumOfDeathDens = 0, sumOfDeathAge = 0, sumOfDeathGDP = 0, sumOfDeathCardioVasc = 0, sumOfDeathTB = 0, sumOfDeathPneu = 0, sumOfDeathBronch = 0, sumOfDeathEmphy = 0, sumOfDeathAsthma = 0, sumOfDeathDiab = 0, sumOfDeathLifeExp = 0, sumOfDeathHDI = 0;
			String correlationOfDeathDens = null, correlationOfDeathAge = null, correlationOfDeathGDP = null, correlationOfDeathCardioVasc = null, correlationOfDeathTB = null, correlationOfDeathPneu = null, correlationOfDeathBronch = null, correlationOfDeathEmphy = null, correlationOfDeathAsthma = null, correlationOfDeathDiab = null, correlationOfDeathLifeExp = null, correlationOfDeathHDI = null;

			while (values.hasNext()) {
				secCount = 0;
				String str = values.next().toString();
				StringTokenizer breakdown = new StringTokenizer(str," ");
				while(breakdown.hasMoreTokens() && secCount < 13) {
					String ind = breakdown.nextToken();
					switch(secCount) {
						case 0: {
							totDeathsD = Double.valueOf(ind);
							break;
						}
						case 1: {
							popDensD = Double.valueOf(ind);
							break;
						}
						case 2: {
							ageD = Double.valueOf(ind);
							break;
						}
						case 3: {
							gdpD = Double.valueOf(ind);	
							break;
						}
						case 4: {
							cardioVascD = Double.valueOf(ind);
							break;
						}
						case 5: {
							tbD = Double.valueOf(ind);
							break;
						}
						case 6: {
							pneuD = Double.valueOf(ind);
							break;
						}
						case 7: {
							bronchD = Double.valueOf(ind);
							break;
						}
						case 8: {
							emphyD = Double.valueOf(ind);
							break;
						}
						case 9: {
							asthmaD = Double.valueOf(ind);
							break;
						}
						case 10: {
							diabD = Double.valueOf(ind);
							break;
						}
						case 11: {
							lifeExpD = Double.valueOf(ind);
							break;
						}
						case 12: {
							hdiD = Double.valueOf(ind);
							break;
						}
						default: {
							break;
						}
					}
					secCount++;
				}
				count++;
				sumOfDeaths += totDeathsD;
				sumOfDens += popDensD;
				sumOfAge += ageD;
				sumOfGDP += gdpD;
				sumOfCardioVasc += cardioVascD;
				sumOfTB += tbD;
				sumOfPneu+= pneuD;
				sumOfBronch+= bronchD;
				sumOfEmphy += emphyD;
				sumOfAsthma+= asthmaD;
				sumOfDiab += diabD;
				sumOfLifeExp+= lifeExpD;
				sumOfHDI+= hdiD;
				
				sqSumOfDeaths += Math.pow(totDeathsD,2);
				sqSumOfDens += Math.pow(popDensD,2);
				sqSumOfAge += Math.pow(ageD,2);
				sqSumOfGDP += Math.pow(gdpD,2);
				sqSumOfCardioVasc += Math.pow(cardioVascD,2);
				sqSumOfTB += Math.pow(tbD,2);
				sqSumOfPneu+= Math.pow(pneuD,2);
				sqSumOfBronch+= Math.pow(bronchD,2);
				sqSumOfEmphy += Math.pow(emphyD,2);
				sqSumOfAsthma+= Math.pow(asthmaD,2);
				sqSumOfDiab += Math.pow(diabD,2);
				sqSumOfLifeExp+= Math.pow(lifeExpD,2);
				sqSumOfHDI+= Math.pow(hdiD,2);
				
				sumOfDeathDens += (totDeathsD * popDensD);
				sumOfDeathAge += (totDeathsD * ageD);
				sumOfDeathGDP += (totDeathsD * gdpD);
				sumOfDeathCardioVasc += (totDeathsD * cardioVascD);
				sumOfDeathTB += (totDeathsD * tbD);
				sumOfDeathPneu += (totDeathsD * pneuD);
				sumOfDeathBronch += (totDeathsD * bronchD);
				sumOfDeathEmphy += (totDeathsD * emphyD);
				sumOfDeathAsthma += (totDeathsD * asthmaD);
				sumOfDeathDiab += (totDeathsD * diabD);
				sumOfDeathLifeExp += (totDeathsD * lifeExpD);
				sumOfDeathHDI += (totDeathsD * hdiD);
			}
		
			sumSqOfDeaths = Math.pow(sumOfDeaths,2);
			sumSqOfDens = Math.pow(sumOfDens,2);
			sumSqOfAge = Math.pow(sumOfAge,2);
			sumSqOfGDP = Math.pow(sumOfGDP,2);
			sumSqOfCardioVasc = Math.pow(sumOfCardioVasc,2);
			sumSqOfTB = Math.pow(sumOfTB,2);
			sumSqOfPneu = Math.pow(sumOfPneu,2);
			sumSqOfBronch = Math.pow(sumOfBronch,2);
			sumSqOfEmphy = Math.pow(sumOfEmphy,2);
			sumSqOfAsthma = Math.pow(sumOfAsthma,2);
			sumSqOfDiab = Math.pow(sumOfDiab,2);
			sumSqOfLifeExp = Math.pow(sumOfLifeExp,2);
			sumSqOfHDI = Math.pow(sumOfHDI,2);

			
			correlationOfDeathDens = String.valueOf(((count*(sumOfDeathDens)) - (sumOfDeaths * sumOfDens)) / Math.sqrt(((count * sqSumOfDeaths) - sumSqOfDeaths) * ((count * sqSumOfDens) - sumSqOfDens)));
			correlationOfDeathAge = String.valueOf(((count*(sumOfDeathAge)) - (sumOfDeaths * sumOfAge)) / Math.sqrt(((count * sqSumOfDeaths) - sumSqOfDeaths) * ((count * sqSumOfAge) - sumSqOfAge)));
			correlationOfDeathGDP = String.valueOf(((count*(sumOfDeathGDP)) - (sumOfDeaths * sumOfGDP)) / Math.sqrt(((count * sqSumOfDeaths) - sumSqOfDeaths) * ((count * sqSumOfGDP) - sumSqOfGDP)));
			correlationOfDeathCardioVasc = String.valueOf(((count*(sumOfDeathCardioVasc)) - (sumOfDeaths * sumOfCardioVasc)) / Math.sqrt(((count * sqSumOfDeaths) - sumSqOfDeaths) * ((count * sqSumOfCardioVasc) - sumSqOfCardioVasc)));
			correlationOfDeathTB = String.valueOf(((count*(sumOfDeathTB)) - (sumOfDeaths * sumOfTB)) / Math.sqrt(((count * sqSumOfDeaths) - sumSqOfDeaths) * ((count * sqSumOfTB) - sumSqOfTB)));
			correlationOfDeathPneu = String.valueOf(((count*(sumOfDeathPneu)) - (sumOfDeaths * sumOfPneu)) / Math.sqrt(((count * sqSumOfDeaths) - sumSqOfDeaths) * ((count * sqSumOfPneu) - sumSqOfPneu)));
			correlationOfDeathBronch = String.valueOf(((count*(sumOfDeathBronch)) - (sumOfDeaths * sumOfBronch)) / Math.sqrt(((count * sqSumOfDeaths) - sumSqOfDeaths) * ((count * sqSumOfBronch) - sumSqOfBronch)));
			correlationOfDeathEmphy = String.valueOf(((count*(sumOfDeathEmphy)) - (sumOfDeaths * sumOfEmphy)) / Math.sqrt(((count * sqSumOfDeaths) - sumSqOfDeaths) * ((count * sqSumOfEmphy) - sumSqOfEmphy)));
			correlationOfDeathAsthma = String.valueOf(((count*(sumOfDeathAsthma)) - (sumOfDeaths * sumOfAsthma)) / Math.sqrt(((count * sqSumOfDeaths) - sumSqOfDeaths) * ((count * sqSumOfAsthma) - sumSqOfAsthma)));
			correlationOfDeathDiab = String.valueOf(((count*(sumOfDeathDiab)) - (sumOfDeaths * sumOfDiab)) / Math.sqrt(((count * sqSumOfDeaths) - sumSqOfDeaths) * ((count * sqSumOfDiab) - sumSqOfDiab)));
			correlationOfDeathLifeExp = String.valueOf(((count*(sumOfDeathLifeExp)) - (sumOfDeaths * sumOfLifeExp)) / Math.sqrt(((count * sqSumOfDeaths) - sumSqOfDeaths) * ((count * sqSumOfLifeExp) - sumSqOfLifeExp)));
			correlationOfDeathHDI = String.valueOf(((count*(sumOfDeathHDI)) - (sumOfDeaths * sumOfHDI)) / Math.sqrt(((count * sqSumOfDeaths) - sumSqOfDeaths) * ((count * sqSumOfHDI) - sumSqOfHDI)));
			
			String value_str = (correlationOfDeathDens).concat(" ".concat(correlationOfDeathAge)).concat(" ".concat(correlationOfDeathGDP)).concat(" ".concat(correlationOfDeathCardioVasc)).concat(" ".concat(correlationOfDeathTB)).concat(" ".concat(correlationOfDeathPneu)).concat(" ".concat(correlationOfDeathBronch)).concat(" ".concat(correlationOfDeathEmphy)).concat(" ".concat(correlationOfDeathAsthma)).concat(" ".concat(correlationOfDeathDiab)).concat(" ".concat(correlationOfDeathLifeExp)).concat(" ".concat(correlationOfDeathHDI));
			
			key_out_text.set(""); 
			value_out_text.set(value_str); 
			output.collect(key_out_text, value_out_text);
		}
	}

/***
 * Map Class III
 * Correlation Module
 ***/
	
	public static class MapClass3 extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

		private Text word = new Text();
		private Text values = new Text();

		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			String line = value.toString();
			StringTokenizer tokens = new StringTokenizer(line," ");
			int counter = 0;
			String key_out = null;
			String totDeathString = null, popDenString = null, ageString = null, gdpString = null;
			String cardioVascString = null, tbString = null, pneuString = null, bronchString = null,emphyString = null;
			String asthmaString = null, diabString = null, lifeExpString = null, hdiString = null;
			int numOfCountriesII = listOfCountriesII.size();
			loop:while (tokens.hasMoreTokens() && counter<19) {
				String str = tokens.nextToken();
				switch (counter) {
					case 2: {
						boolean countryAdded = false;
						if(numOfCountriesII == 0) {
							listOfCountriesII.add(str);	
						} else {
							for(int g=0; g<numOfCountriesII; g++) {
								if(str.equals(listOfCountriesII.get(g))) {
									countryAdded = true;
									break;
								}
							}
						}
						if(countryAdded == false) {
							listOfCountriesII.add(str);
						}
						key_out = listOfCountriesII.get((listOfCountriesII.size()==0?0:listOfCountriesII.size()-1));
						break;
					}
				
					case 4: {
						totDeathString = str;
						break;
					}
					case 7: {
						popDenString = str;
						break;
					}
					case 8: {
						ageString = str;
						break;
					}
					case 9: {
						gdpString = str;	
						break;
					}
					case 10: {
						cardioVascString = str;
						break;
					}
					case 11: {
						tbString = str;
						break;
					}
					case 12: {
						pneuString = str;
						break;
					}
					case 13: {
						bronchString = str;
						break;
					}
					case 14: {
						emphyString = str;
						break;
					}
					case 15: {
						asthmaString = str;
						break;
					}
					case 16: {
						diabString = str;
						break;
					}
					case 17: {
						lifeExpString = str;
						break;
					}
					case 18: {
						hdiString = str;
						break;
					}
					default: {
						break;
					}
				}
				counter++;
			}
			String value_str = (totDeathString.concat(" ".concat(popDenString))).concat(" ".concat(ageString)).concat(" ".concat(gdpString.concat(" ".concat(cardioVascString)))).concat(" ".concat(tbString)).concat(" ".concat(pneuString.concat(" ".concat(bronchString)))).concat(" ".concat(emphyString)).concat(" ".concat(asthmaString.concat(" ".concat(diabString)))).concat(" ".concat(lifeExpString)).concat(" ".concat(hdiString));
			word.set(key_out);
			values.set(value_str);
			output.collect(word, values);
		}
	}

	/**
	 * Reduce Class III
	 **/

	public static class ReduceClass3 extends MapReduceBase
			implements Reducer<Text, Text, Text, Text> {
		private Text value_out_text = new Text();
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			String totDeathString = null, popDenString = null, ageString = null, gdpString = null;
			String cardioVascString = null, tbString = null, pneuString = null, bronchString = null,emphyString = null;
			String asthmaString = null, diabString = null, lifeExpString = null, hdiString = null;
			int count = 0; int secCount = 0;
			double totDeathsD = 0, popDensD = 0, ageD = 0, gdpD = 0, cardioVascD = 0, tbD = 0, pneuD = 0, bronchD = 0, emphyD = 0, asthmaD = 0, diabD = 0, lifeExpD = 0, hdiD = 0;
			double sumOfDeaths = 0, sumOfDens = 0, sumOfAge = 0, sumOfGDP = 0, sumOfCardioVasc = 0, sumOfTB = 0, sumOfPneu = 0, sumOfBronch = 0, sumOfEmphy = 0, sumOfAsthma = 0, sumOfDiab = 0, sumOfLifeExp = 0, sumOfHDI = 0;
			double sqSumOfDeaths = 0, sqSumOfDens = 0, sqSumOfAge = 0, sqSumOfGDP = 0, sqSumOfCardioVasc = 0, sqSumOfTB = 0, sqSumOfPneu = 0, sqSumOfBronch = 0, sqSumOfEmphy = 0, sqSumOfAsthma = 0, sqSumOfDiab = 0, sqSumOfLifeExp = 0, sqSumOfHDI = 0;
			double sumSqOfDeaths = 0, sumSqOfDens = 0, sumSqOfAge = 0, sumSqOfGDP = 0, sumSqOfCardioVasc = 0, sumSqOfTB = 0, sumSqOfPneu = 0, sumSqOfBronch = 0, sumSqOfEmphy = 0, sumSqOfAsthma = 0, sumSqOfDiab = 0, sumSqOfLifeExp = 0, sumSqOfHDI = 0;
			double deathDens = 0, deathAge = 0, deathGDP = 0, deathCardioVasc = 0, deathTB = 0, deathPneu = 0, deathBronch = 0, deathEmphy = 0, deathAsthma = 0, deathDiab = 0, deathLifeExp = 0, deathHDI = 0;
			double sumOfDeathDens = 0, sumOfDeathAge = 0, sumOfDeathGDP = 0, sumOfDeathCardioVasc = 0, sumOfDeathTB = 0, sumOfDeathPneu = 0, sumOfDeathBronch = 0, sumOfDeathEmphy = 0, sumOfDeathAsthma = 0, sumOfDeathDiab = 0, sumOfDeathLifeExp = 0, sumOfDeathHDI = 0;
			String correlationOfDeathDens = null, correlationOfDeathAge = null, correlationOfDeathGDP = null, correlationOfDeathCardioVasc = null, correlationOfDeathTB = null, correlationOfDeathPneu = null, correlationOfDeathBronch = null, correlationOfDeathEmphy = null, correlationOfDeathAsthma = null, correlationOfDeathDiab = null, correlationOfDeathLifeExp = null, correlationOfDeathHDI = null;
			
			while (values.hasNext()) {
				secCount = 0;
				String str = values.next().toString();
				StringTokenizer breakdown = new StringTokenizer(str," ");
				while(breakdown.hasMoreTokens() && secCount < 13) {
					String ind = breakdown.nextToken();
					switch(secCount) {
						case 0: {
							totDeathsD = Double.valueOf(ind);
							break;
						}
						case 1: {
							popDensD = Double.valueOf(ind);
							break;
						}
						case 2: {
							ageD = Double.valueOf(ind);
							break;
						}
						case 3: {
							gdpD = Double.valueOf(ind);	
							break;
						}
						case 4: {
							cardioVascD = Double.valueOf(ind);
							break;
						}
						case 5: {
							tbD = Double.valueOf(ind);
							break;
						}
						case 6: {
							pneuD = Double.valueOf(ind);
							break;
						}
						case 7: {
							bronchD = Double.valueOf(ind);
							break;
						}
						case 8: {
							emphyD = Double.valueOf(ind);
							break;
						}
						case 9: {
							asthmaD = Double.valueOf(ind);
							break;
						}
						case 10: {
							diabD = Double.valueOf(ind);
							break;
						}
						case 11: {
							lifeExpD = Double.valueOf(ind);
							break;
						}
						case 12: {
							hdiD = Double.valueOf(ind);
							break;
						}
						default: {
							break;
						}
					}
					secCount++;
				}
				count++;
				sumOfDeaths += totDeathsD;
				sumOfDens += popDensD;
				sumOfAge += ageD;
				sumOfGDP += gdpD;
				sumOfCardioVasc += cardioVascD;
				sumOfTB += tbD;
				sumOfPneu+= pneuD;
				sumOfBronch+= bronchD;
				sumOfEmphy += emphyD;
				sumOfAsthma+= asthmaD;
				sumOfDiab += diabD;
				sumOfLifeExp+= lifeExpD;
				sumOfHDI+= hdiD;
				
				sqSumOfDeaths += Math.pow(totDeathsD,2);
				sqSumOfDens += Math.pow(popDensD,2);
				sqSumOfAge += Math.pow(ageD,2);
				sqSumOfGDP += Math.pow(gdpD,2);
				sqSumOfCardioVasc += Math.pow(cardioVascD,2);
				sqSumOfTB += Math.pow(tbD,2);
				sqSumOfPneu+= Math.pow(pneuD,2);
				sqSumOfBronch+= Math.pow(bronchD,2);
				sqSumOfEmphy += Math.pow(emphyD,2);
				sqSumOfAsthma+= Math.pow(asthmaD,2);
				sqSumOfDiab += Math.pow(diabD,2);
				sqSumOfLifeExp+= Math.pow(lifeExpD,2);
				sqSumOfHDI+= Math.pow(hdiD,2);
				
				sumOfDeathDens += (totDeathsD * popDensD);
				sumOfDeathAge += (totDeathsD * ageD);
				sumOfDeathGDP += (totDeathsD * gdpD);
				sumOfDeathCardioVasc += (totDeathsD * cardioVascD);
				sumOfDeathTB += (totDeathsD * tbD);
				sumOfDeathPneu += (totDeathsD * pneuD);
				sumOfDeathBronch += (totDeathsD * bronchD);
				sumOfDeathEmphy += (totDeathsD * emphyD);
				sumOfDeathAsthma += (totDeathsD * asthmaD);
				sumOfDeathDiab += (totDeathsD * diabD);
				sumOfDeathLifeExp += (totDeathsD * lifeExpD);
				sumOfDeathHDI += (totDeathsD * hdiD);
			}
	
			sumSqOfDeaths = Math.pow(sumOfDeaths,2);
			sumSqOfDens = Math.pow(sumOfDens,2);
			sumSqOfAge = Math.pow(sumOfAge,2);
			sumSqOfGDP = Math.pow(sumOfGDP,2);
			sumSqOfCardioVasc = Math.pow(sumOfCardioVasc,2);
			sumSqOfTB = Math.pow(sumOfTB,2);
			sumSqOfPneu = Math.pow(sumOfPneu,2);
			sumSqOfBronch = Math.pow(sumOfBronch,2);
			sumSqOfEmphy = Math.pow(sumOfEmphy,2);
			sumSqOfAsthma = Math.pow(sumOfAsthma,2);
			sumSqOfDiab = Math.pow(sumOfDiab,2);
			sumSqOfLifeExp = Math.pow(sumOfLifeExp,2);
			sumSqOfHDI = Math.pow(sumOfHDI,2);
			
			correlationOfDeathDens = String.valueOf(((count*sumOfDeathDens) - (sumOfDeaths * sumOfDens)) / Math.sqrt(((count * sqSumOfDeaths) - sumSqOfDeaths) * ((count * sqSumOfDens) - sumSqOfDens)));
			correlationOfDeathAge = String.valueOf(((count*(sumOfDeathAge)) - (sumOfDeaths * sumOfAge)) / Math.sqrt(((count * sqSumOfDeaths) - sumSqOfDeaths) * ((count * sqSumOfAge) - sumSqOfAge)));
			correlationOfDeathGDP = String.valueOf(((count*(sumOfDeathGDP)) - (sumOfDeaths * sumOfGDP)) / Math.sqrt(((count * sqSumOfDeaths) - sumSqOfDeaths) * ((count * sqSumOfGDP) - sumSqOfGDP)));
			correlationOfDeathCardioVasc = String.valueOf(((count*(sumOfDeathCardioVasc)) - (sumOfDeaths * sumOfCardioVasc)) / Math.sqrt(((count * sqSumOfDeaths) - sumSqOfDeaths) * ((count * sqSumOfCardioVasc) - sumSqOfCardioVasc)));
			correlationOfDeathTB = String.valueOf(((count*(sumOfDeathTB)) - (sumOfDeaths * sumOfTB)) / Math.sqrt(((count * sqSumOfDeaths) - sumSqOfDeaths) * ((count * sqSumOfTB) - sumSqOfTB)));
			correlationOfDeathPneu = String.valueOf(((count*(sumOfDeathPneu)) - (sumOfDeaths * sumOfPneu)) / Math.sqrt(((count * sqSumOfDeaths) - sumSqOfDeaths) * ((count * sqSumOfPneu) - sumSqOfPneu)));
			correlationOfDeathBronch = String.valueOf(((count*(sumOfDeathBronch)) - (sumOfDeaths * sumOfBronch)) / Math.sqrt(((count * sqSumOfDeaths) - sumSqOfDeaths) * ((count * sqSumOfBronch) - sumSqOfBronch)));
			correlationOfDeathEmphy = String.valueOf(((count*(sumOfDeathEmphy)) - (sumOfDeaths * sumOfEmphy)) / Math.sqrt(((count * sqSumOfDeaths) - sumSqOfDeaths) * ((count * sqSumOfEmphy) - sumSqOfEmphy)));
			correlationOfDeathAsthma = String.valueOf(((count*(sumOfDeathAsthma)) - (sumOfDeaths * sumOfAsthma)) / Math.sqrt(((count * sqSumOfDeaths) - sumSqOfDeaths) * ((count * sqSumOfAsthma) - sumSqOfAsthma)));
			correlationOfDeathDiab = String.valueOf(((count*(sumOfDeathDiab)) - (sumOfDeaths * sumOfDiab)) / Math.sqrt(((count * sqSumOfDeaths) - sumSqOfDeaths) * ((count * sqSumOfDiab) - sumSqOfDiab)));
			correlationOfDeathLifeExp = String.valueOf(((count*(sumOfDeathLifeExp)) - (sumOfDeaths * sumOfLifeExp)) / Math.sqrt(((count * sqSumOfDeaths) - sumSqOfDeaths) * ((count * sqSumOfLifeExp) - sumSqOfLifeExp)));
			correlationOfDeathHDI = String.valueOf(((count*(sumOfDeathHDI)) - (sumOfDeaths * sumOfHDI)) / Math.sqrt(((count * sqSumOfDeaths) - sumSqOfDeaths) * ((count * sqSumOfHDI) - sumSqOfHDI)));
				
			String value_str = (correlationOfDeathDens).concat(" ".concat(correlationOfDeathAge)).concat(" ".concat(correlationOfDeathGDP)).concat(" ".concat(correlationOfDeathCardioVasc)).concat(" ".concat(correlationOfDeathTB)).concat(" ".concat(correlationOfDeathPneu)).concat(" ".concat(correlationOfDeathBronch)).concat(" ".concat(correlationOfDeathEmphy)).concat(" ".concat(correlationOfDeathAsthma)).concat(" ".concat(correlationOfDeathDiab)).concat(" ".concat(correlationOfDeathLifeExp)).concat(" ".concat(correlationOfDeathHDI));
			
			value_out_text.set(value_str); 
			output.collect(key, value_out_text);
		}
	}

/***
 * Map Class IV
 * Vaccination Module
 ***/
	public static class MapClass4 extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

		private Text word = new Text();
		private Text values = new Text();

		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			String line = value.toString();
			StringTokenizer tokens = new StringTokenizer(line,",");
			int counter = 0, c2 = 0, days = 0;
			String str, country="", date="", tot_vacc="", daily_vacc="";
			String key_out = null;
			boolean countryAdded = false;
			int numOfCountriesIII = listOfCountriesIII.size();
			loop:while (tokens.hasMoreTokens()) {
				str = tokens.nextToken();;
				switch (counter) {	
					case 0: {
						countryAdded = false;
						country = str; 
						for(int g=0; g<numOfCountriesIII; g++) {
							if(country.equals(listOfCountriesIII.get(g))) {
								countryAdded = true;
								break;
							}
						}
						if(countryAdded == false) {
							listOfCountriesIII.add(country);
						}
						key_out = listOfCountriesIII.get((listOfCountriesIII.size()==0?0:listOfCountriesIII.size()-1));
						break;
					}
					case 2: {
						date = str;
						if(countryAdded == false) {
							date_copy = date;
						}
						break;
					}
					case 3: {
						tot_vacc = str;
						break;
					}
					case 4: {
						daily_vacc = str;	
						break;
					}
					default: {
						break;
					}
				}
				counter++;
			}

			if(!date.equals(date_copy)) {
				String dateArr[] = date.split("-");
				String dateCArr[] = date_copy.split("-");
				int daytodays = Integer.parseInt(dateArr[0])-Integer.parseInt(dateCArr[0]);
				int mntodays = (Integer.parseInt(dateArr[1])-Integer.parseInt(dateCArr[1]))*30;
				int yrtodays = (Integer.parseInt(dateArr[2])-Integer.parseInt(dateCArr[2]))*365;
				days = daytodays + mntodays
						+ yrtodays + 1;
			} else {
				days = 1;
			}
			
			String value_str =(days+" ").concat(tot_vacc+" ").concat(daily_vacc);
			word.set(key_out);
			values.set(value_str);
			output.collect(word, values);
		}
	}
	
/**
 * Reduce Class IV
 * Vaccination Module
 **/

	public static class ReduceClass4 extends MapReduceBase
			implements Reducer<Text, Text, Text, Text> {
		private Text value_out_text = new Text();
		private String value_str;
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			int count = 0, secCount = 0;	
				double numOfDays=0, tot_vacc=0, daily_vacc=0;
				double x_sum = 0, x_mean = 0, tot_vacc_sum = 0, tot_vacc_mean = 0, daily_vacc_sum = 0, daily_vacc_mean = 0,
					   sigma_x = 0, sigma_tot_vacc = 0, sigma_daily_vacc = 0, r_x_tot_vacc = 0, r_x_daily_vacc = 0, b_x_tot_vacc = 0,
					   b_x_daily_vacc = 0, a_x_tot_vacc = 0, a_x_daily_vacc = 0, y_x_tot_vacc = 0, y_x_daily_vacc = 0;
				double x_min_mean = 0, tot_vacc_min_mean = 0, daily_vacc_min_mean = 0;
				double x_min_mean_sum = 0, tot_vacc_min_mean_sum = 0, daily_vacc_min_mean_sum = 0;
				double x_min_mean_sq_sum = 0, tot_vacc_min_mean_sq_sum = 0, daily_vacc_min_mean_sq_sum = 0;
				double sumOfXMinMeanTotVaccMinMean = 0, sumOfXMinMeanDailyVaccMinMean = 0;
				int xListSize;
			while (values.hasNext()) {
				secCount = 0;
				String str = values.next().toString();
				StringTokenizer breakdown = new StringTokenizer(str," ");
				while(breakdown.hasMoreTokens() && secCount < 3) {
					String ind = breakdown.nextToken();
					switch(secCount) {
						case 0: {
							numOfDays = Double.parseDouble(ind);
							break;
						}
						case 1: {
							tot_vacc = Double.parseDouble(ind);
							break;
						}
						case 2: {
							daily_vacc = Double.parseDouble(ind);
							break;
						}
						default: {
							break;
						}
					}
					secCount++;
				} 
				count++;

				listOfX_es.add(numOfDays);
				listOfTot_Vacc_es.add(tot_vacc);
				listOfDaily_Vacc_es.add(daily_vacc);
				x_sum = x_sum + numOfDays;
				tot_vacc_sum = tot_vacc_sum + tot_vacc;
				daily_vacc_sum = daily_vacc_sum + daily_vacc;
			}
			x_mean = x_sum/count;
			tot_vacc_mean = tot_vacc_sum/count;
			daily_vacc_mean = daily_vacc_sum/count;

			xListSize = listOfX_es.size();
			for(int x=0; x<xListSize; x++) {
				x_min_mean = listOfX_es.get(x)-x_mean;
				x_min_mean_sq_sum = x_min_mean_sq_sum + Math.pow(x_min_mean, 2);
				tot_vacc_min_mean = listOfTot_Vacc_es.get(x)-tot_vacc_mean;	
				tot_vacc_min_mean_sq_sum = tot_vacc_min_mean_sq_sum + Math.pow(tot_vacc_min_mean, 2);
				daily_vacc_min_mean = listOfDaily_Vacc_es.get(x)-daily_vacc_mean;	
				daily_vacc_min_mean_sq_sum = daily_vacc_min_mean_sq_sum + Math.pow(daily_vacc_min_mean, 2);
				sumOfXMinMeanTotVaccMinMean = sumOfXMinMeanTotVaccMinMean + (x_min_mean*tot_vacc_min_mean);
				sumOfXMinMeanDailyVaccMinMean = sumOfXMinMeanDailyVaccMinMean + (x_min_mean*daily_vacc_min_mean);
			}

			sigma_x = Math.sqrt((x_min_mean_sq_sum/(count-1)));
			sigma_tot_vacc = Math.sqrt((tot_vacc_min_mean_sq_sum/(count-1)));
			sigma_daily_vacc = Math.sqrt((daily_vacc_min_mean_sq_sum/(count-1)));

			r_x_tot_vacc = sumOfXMinMeanTotVaccMinMean / Math.sqrt(x_min_mean_sq_sum*tot_vacc_min_mean_sq_sum);
			r_x_daily_vacc = sumOfXMinMeanDailyVaccMinMean / Math.sqrt(x_min_mean_sq_sum*daily_vacc_min_mean_sq_sum);

			b_x_tot_vacc = r_x_tot_vacc * (sigma_tot_vacc/sigma_x);
			b_x_daily_vacc = r_x_daily_vacc * (sigma_daily_vacc/sigma_x);

			a_x_tot_vacc = tot_vacc_mean - (b_x_tot_vacc*x_mean);
			a_x_daily_vacc = daily_vacc_mean - (b_x_daily_vacc*x_mean);

			y_x_tot_vacc = a_x_tot_vacc + (b_x_tot_vacc*daysToPredict);
			y_x_daily_vacc = a_x_daily_vacc + (b_x_daily_vacc*daysToPredict);

			listOfX_es.clear();
			listOfTot_Vacc_es.clear();
			listOfDaily_Vacc_es.clear();

			value_str = y_x_tot_vacc+" "+y_x_daily_vacc;
			value_out_text.set(value_str); 
			output.collect(key, value_out_text);
		}
	}
	static int printUsage() {
		System.out.println("Corrid [-m <maps>] [-r <reduces>] <job_1 input> <job_1 output> <job_2 output>");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	public int run(String[] args) throws Exception {
		Configuration config = getConf();
			
		JobConf conf = new JobConf(config, Corrid.class);
		conf.setJobName("Job1");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);

		conf.setMapperClass(MapClass.class);

		conf.setReducerClass(ReduceClass.class);
		List<String> other_args = new ArrayList<String>();
		for(int i=0; i < args.length; ++i) {
			try {
				if ("-m".equals(args[i])) {
					conf.setNumMapTasks(Integer.parseInt(args[++i]));
				} else if ("-r".equals(args[i])) {
					conf.setNumReduceTasks(Integer.parseInt(args[++i]));
				} else {
					other_args.add(args[i]);
				}
			} catch (NumberFormatException except) {
				System.out.println("ERROR: Integer expected instead of " + args[i]);
				return printUsage();
			} catch (ArrayIndexOutOfBoundsException except) {
				System.out.println("ERROR: Required parameter missing from " +
						args[i-1]);
				return printUsage();
			}
		}

		FileInputFormat.setInputPaths(conf, other_args.get(0));
		FileOutputFormat.setOutputPath(conf, new Path(other_args.get(2)));

		JobClient.runJob(conf);
		
		JobConf conf2 = new JobConf(config, Corrid.class);
		conf.setJobName("Job2");

		conf2.setOutputKeyClass(Text.class);
		conf2.setOutputValueClass(Text.class);
		
		conf2.setMapOutputKeyClass(Text.class);
		conf2.setMapOutputValueClass(Text.class);

		conf2.setMapperClass(MapClass2.class);

		conf2.setReducerClass(ReduceClass2.class);
		
		FileInputFormat.setInputPaths(conf2, other_args.get(2).concat("/part-00000"));
		FileOutputFormat.setOutputPath(conf2, new Path(other_args.get(2).concat("/Worldwide_Result")));

		JobClient.runJob(conf2);
		System.out.println("Worldwide results done!");

		JobConf conf3 = new JobConf(config, Corrid.class);
		conf.setJobName("Job3");

		conf3.setOutputKeyClass(Text.class);
		conf3.setOutputValueClass(Text.class);
		
		conf3.setMapOutputKeyClass(Text.class);
		conf3.setMapOutputValueClass(Text.class);

		conf3.setMapperClass(MapClass3.class);

		conf3.setReducerClass(ReduceClass3.class);
		
		FileInputFormat.setInputPaths(conf3, other_args.get(0));
		FileOutputFormat.setOutputPath(conf3, new Path(other_args.get(2).concat("/Contries_Result")));

		JobClient.runJob(conf3);
		System.out.println("Country-wise results done!");

		JobConf conf4 = new JobConf(config, Corrid.class);
		conf.setJobName("Job4");

		conf4.setOutputKeyClass(Text.class);
		conf4.setOutputValueClass(Text.class);
		
		conf4.setMapOutputKeyClass(Text.class);
		conf4.setMapOutputValueClass(Text.class);

		conf4.setMapperClass(MapClass4.class);

		conf4.setReducerClass(ReduceClass4.class);
		
		FileInputFormat.setInputPaths(conf4, other_args.get(1));
		FileOutputFormat.setOutputPath(conf4, new Path(other_args.get(2).concat("/Vaccination_Result")));

		Scanner gkScan = new Scanner(System.in);

		System.out.print("Enter the number of days for which the prediction has to be made: ");
		daysToPredict = gkScan.nextInt();
		
		while(!(daysToPredict>0)) {
			System.out.print("Please input a value >0: ");
			daysToPredict = gkScan.nextInt();
		}
		JobClient.runJob(conf4);
		System.out.println("Vaccine results done!");
		return 0;
	}


	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Corrid(), args);
		System.exit(res);
	}

}
