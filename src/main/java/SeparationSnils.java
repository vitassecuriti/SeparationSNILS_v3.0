import org.apache.maven.plugin.lifecycle.Execution;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by VSKryukov on 02.07.2015.
 * v1.2 :добавил формирование файла с параметрами выборки в названии
 * v1.3 :Добавил обработку строк некоректного формата
 * v2.0 :Изменил структуру хранения считанных данных
 *      :Добавил код отсеивания лишних записей по регионам
 *      :Добавил фильтрацию дублей во входных данных
 *      :Добавил сокращение эталонного списка после итерации
 * v2.1 :Добавил Отлов некорректных данных в списках и выведение их в отдельный список
 * v2.2 :Refactoring перенес запись в файл в отдельную процу. Почистил мусор
 * v2.3 :Добавил обработку некорректных записей первого списка, в случае, когда число лексем меньше количества
 *      :полей класса SNILS
 *      :Добавил обработку некорректных полей второго списка.
 *      :Добавил запись некорректных данных второго списка в отдельный файл.
 * v3.0 :Изменил алгоритм разбора данных из файла
 *      :Если коллекции по регионам после сепарирования пустые, файлы не создаются
 */
public class SeparationSnils {
    public static final String EXECUTION_DIR = new File("").getAbsolutePath();
    public static String certStartDate;
    public static String certEndDate;

    public static class SNILS {
        private String csn;                         //Certificate SerialNumber
        // private String ssn;                         //Subject SerialNumber
        private String city;                        //Subject L
        private String cn;                          //Subject CN
        private String notBefore;                   //CertificateNotBefore
        private String notAfter;                    //CertificateNotAfter
        //private String organization;                //Subject O
        private String region;                      //Subject S
        // private String inn;                         //Subject|SubjectAltName INN
        private String snilsNumber;                 //Subject|SubjectAltName SNILS


        public SNILS (String[] arr){
            this.csn = arr[0];
            StringBuilder adderss = new StringBuilder();

            for (int i=1;i<arr.length - 5;i++ ){
                adderss.append(" ").append(arr[i].trim());
            }
            this.city = adderss.toString().trim();
            this.cn = arr[arr.length - 5];
            this.notBefore = arr[arr.length - 4];
            this.notAfter = arr[arr.length - 3];
            this.region = arr[arr.length - 2];
            this.snilsNumber = arr[arr.length - 1];

        }

        @Override
        public String toString(){
            return new StringBuilder()

                    .append(csn).append(";")
                    .append(city).append(";")
                    .append(cn).append(";")
                    .append(notBefore).append(";")
                    .append(notAfter).append(";")
                    .append(region).append(";")
                    .append(snilsNumber)
                    .toString();
        }


        @Override
        public int hashCode() {
            return snilsNumber.hashCode();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SNILS snils = (SNILS) o;
            if (!snilsNumber.equals(snils.snilsNumber)) return false;
            return true;
        }
    }


    public static void main(String[] args) throws FileNotFoundException {

        if (args.length!=1){
            System.out.println("Not Parameter");
        }

        Integer regionSortVrn = 36;
        Integer regionSortMrm = 51;
        Integer regionSortYNAO = 89;
        Integer regionSortHab = 27;
        Integer regionSortSah = 65;
        Integer regionSortPerm = 59;

        Long start_time = System.currentTimeMillis();
        Map<Long, SNILS> deleteList = new HashMap<>();
        LinkedList<String> incorrectListSnils = new LinkedList<>();
        LinkedList<String> wrangListSnilsSecondList = new LinkedList<>();

        Scanner scanner = new Scanner(new File ("C:\\1.txt"),"UTF-8");

        //Вытаскиваем параметры фильтров
        String filterLine = scanner.nextLine();
        String [] parseFilterLine = filterLine.split(";");
        certStartDate = getDateFromStr(parseFilterLine[0]);
        certEndDate = getDateFromStr(parseFilterLine[1]);

        scanner.nextLine();//игнорим параметры в шапке файла и пустую строку

        String header = scanner.nextLine(); //запоминаем шапку таблицы

        int countStr = 0;
        //читаем данные файла
        while(scanner.hasNext()){
            //System.out.println("!!!!!! - " + countStr);
            String line = scanner.nextLine();
            String[] parsedLine = line.split(",");
            //String[] parsedRegionLine = parsedLine[5].split(" ");

            try {
                String[] parsedRegionLine = parsedLine[parsedLine.length - 2].split(" ");
                //Проверяем, что нет незаполненного региона в списке 1
                Integer.parseInt(parsedRegionLine[0].replace("\"", ""));
                //Проверяем, что нет незаполненного снилса в списке 1
                Long.parseLong(parsedLine[parsedLine.length - 1].replace("\"", ""));
               SNILS sb=  new SNILS(parsedLine);
            if (parsedLine.length < 7){
                    incorrectListSnils.add(line);

            } else  if ((Integer.parseInt(parsedRegionLine[0].replace("\"", "")) == regionSortVrn) ||
                    (Integer.parseInt(parsedRegionLine[0].replace("\"", "")) == regionSortMrm) ||
                    (Integer.parseInt(parsedRegionLine[0].replace("\"", "")) == regionSortYNAO) ||
                    (Integer.parseInt(parsedRegionLine[0].replace("\"", "")) == regionSortHab) ||
                    (Integer.parseInt(parsedRegionLine[0].replace("\"", "")) == regionSortSah) ||
                    (Integer.parseInt(parsedRegionLine[0].replace("\"", "")) == regionSortPerm)) {

                    deleteList.put(Long.parseLong(parsedLine[parsedLine.length - 1].replace("\"", "")), new SNILS(parsedLine));

            }
            countStr++;
            //System.out.println(countStr + " - " + Long.parseLong(parsedLine[6].replace("\"", "")) + line);
            } catch (Exception e) {
                System.out.println(e.toString());

                incorrectListSnils.add(line);

            }
        }
        scanner.close();
        System.out.println("In file 1.txt it is found records - " + countStr);
        System.out.println("From 1.txt it is loaded records - " + deleteList.size());



        LinkedList<Long> expectedListFull = new LinkedList<>();
        scanner = new Scanner(new File ("C:\\2.txt"),"UTF-8");
        scanner.nextLine(); //игнорируем шапку

        //читаем данные из файла
        while(scanner.hasNext()){
            String line = scanner.nextLine();

            try {
                Long.parseLong(line.replace("\"", ""));
                expectedListFull.add(Long.parseLong(line.replace("\"", "")));
            } catch (Exception e){
                wrangListSnilsSecondList.add(line);
            }



        }
        scanner.close();

        System.out.println("In file 2 it is found records - " + expectedListFull.size());
        LinkedList<Long> expectedList =  DeleteDouble(expectedListFull);
        System.out.println("From 2.txt it is loaded records - " + expectedList.size());

        LinkedList<SNILS> indexList = new LinkedList<>();
        LinkedList<SNILS> indexListVRN = new LinkedList<>();
        LinkedList<SNILS> indexListMrm = new LinkedList<>();
        LinkedList<SNILS> indexListYNAO = new LinkedList<>();
        LinkedList<SNILS> indexListHab = new LinkedList<>();
        LinkedList<SNILS> indexListSah = new LinkedList<>();
        LinkedList<SNILS> indexListPerm = new LinkedList<>();


        String strRegion = new StringBuilder()
                .append("Данные разделяем по сл регионам: ")
                .append(regionSortVrn).append(",")
                .append(regionSortMrm).append(",")
                .append(regionSortYNAO).append(",")
                .append(regionSortHab).append(",")
                .append(regionSortSah).append(",")
                .append(regionSortPerm).append(".")
                .append("\n")
                .toString();

        System.out.println(strRegion);




        System.out.println("expectedList.size() befor - " + expectedList.size());


        boolean flag = true;
        System.out.println("---------------------- Beginning process of separation of SNILS ------------------------------");

            for (Map.Entry<Long, SNILS> deleteSnils : deleteList.entrySet()){
                    Long key = deleteSnils.getKey();
                    SNILS value = deleteSnils.getValue();

            Iterator<Long> iterator = expectedList.iterator();//получение итератора для списка

            while (iterator.hasNext())      //проверка, есть ли ещё элементы
            {
                //получение текущего элемента и переход на следующий
                Long expectedSnils = iterator.next();
                   if (key.equals(expectedSnils)){
                    System.out.println("SNILS \"" + expectedSnils +"\" is found in the list 1" );
                    iterator.remove();
                    flag = false;
                }
            }

            if (flag){
                indexList.add(value);
                String[] parsedRegionLine = value.region.split(" ");
                if (Integer.parseInt(parsedRegionLine[0].replace("\"", "")) == regionSortVrn){
                    indexListVRN.add(value);
                    System.out.println("SNILS \"" + value.snilsNumber + "\" is added to the final list VRN" );
                } else if (Integer.parseInt(parsedRegionLine[0].replace("\"", "")) == regionSortMrm){
                    indexListMrm.add(value);
                    System.out.println("SNILS \"" + value.snilsNumber + "\" is added to the final list Mrm" );
                } else if (Integer.parseInt(parsedRegionLine[0].replace("\"", "")) == regionSortYNAO){
                    indexListYNAO.add(value);
                    System.out.println("SNILS \"" + value.snilsNumber + "\" is added to the final list YNAO" );
                } else if (Integer.parseInt(parsedRegionLine[0].replace("\"", "")) == regionSortHab){
                    indexListHab.add(value);
                    System.out.println("SNILS \"" + value.snilsNumber + "\" is added to the final list Hab" );
                } else if (Integer.parseInt(parsedRegionLine[0].replace("\"", "")) == regionSortSah){
                    indexListSah.add(value);
                    System.out.println("SNILS \"" + value.snilsNumber + "\" is added to the final list Sah" );
                } else if (Integer.parseInt(parsedRegionLine[0].replace("\"", "")) == regionSortPerm){
                    indexListPerm.add(value);
                    System.out.println("SNILS \"" + value.snilsNumber + "\" is added to the final list Perm" );
                }
                System.out.println("SNILS \"" + value.snilsNumber + "\" is added to the final list" );
            } else {
                flag = true;
            }
        }
       // System.out.println("expectedList.size() after - " + expectedList.size());
        System.out.println("----------------------- End of process of separation of SNILS -------------------------------");

        System.out.println("----------------------------- Record of results in files ------------------------------------");


        //---------------Запись общего файла -------------------
        WriteToFileSnilsList ("C:\\", "FinalSnilsList_"+ certStartDate + "_" + certEndDate + ".txt", header, indexList);

        //--Запись файла для Воронежа
        WriteToFileSnilsList ("C:\\", "VRN_"+ certStartDate + "_" + certEndDate + ".txt", header, indexListVRN);

        //--Запись файла для
        WriteToFileSnilsList ("C:\\", "Mrm_"+ certStartDate + "_" + certEndDate + ".txt", header, indexListMrm);

        //--Запись файла для ЯНАО
        WriteToFileSnilsList ("C:\\", "YNAO_"+ certStartDate + "_" + certEndDate + ".txt", header, indexListYNAO);

        //--Запись файла для Хабаровска
        WriteToFileSnilsList ("C:\\", "Hab_"+ certStartDate + "_" + certEndDate + ".txt", header, indexListHab);

        //--Запись файла для Сахалина
        WriteToFileSnilsList ("C:\\", "Sah_"+ certStartDate + "_" + certEndDate + ".txt", header, indexListSah);

        //--Запись файла для Пермский край
        WriteToFileSnilsList ("C:\\", "Perm_"+ certStartDate + "_" + certEndDate + ".txt", header, indexListPerm);

        //--Запись файла c некорректными строками
        WriteToFileSnilsString ("C:\\", "IncorrectSnils_"+ certStartDate + "_" + certEndDate + ".txt", header, incorrectListSnils);

        //--Запись файла cо строками второго списка, которые не получилось распарсить
        WriteToFileSnilsString ("C:\\", "wrangListSnilsSecondList"+ certStartDate + "_" + certEndDate + ".txt", header, wrangListSnilsSecondList);


        Long end_time = System.currentTimeMillis();
        System.out.println("Duration - " + (end_time - start_time));
    }

    public static void WriteToFileSnilsList (String path, String nameFile, String header, LinkedList<SNILS> snilsList){
        if (snilsList.size()!=0) {
            String pathName = new StringBuilder()
                    .append(path)
                    .append(nameFile)
                    .toString();

            try (FileWriter writer = new FileWriter(pathName, false)) {
                // запись всей строки
                System.out.println(pathName + " it is written down records - " + snilsList.size());
                writer.write(header + "\n");
                for (SNILS snils : snilsList)
                    writer.write(snils.toString() + "\n");
            } catch (IOException ex) {
                System.out.println(ex.getMessage());
            }
        }
    }

    public static void WriteToFileSnilsString (String path, String nameFile, String header, LinkedList<String> snilsList){

        if (snilsList.size()!=0) {
            String pathName = new StringBuilder()
                    .append(path)
                    .append(nameFile)
                    .toString();

            try (FileWriter writer = new FileWriter(pathName, false)) {
                // запись всей строки
                System.out.println(pathName + " it is written down records - " + snilsList.size());
                writer.write(header + "\n");
                for (String snils : snilsList)
                    writer.write(snils.toString() + "\n");
            } catch (IOException ex) {
                System.out.println(ex.getMessage());
            }
        }
    }


    public static <T> LinkedList<T> DeleteDouble(List<T> listSnils){
        return new LinkedList<>(new HashSet<>(listSnils));
    }


    public static String  getDateFromStr (String strDate) {

        String pattern="\\d{1,2}\\.\\d{1,2}\\.\\d{4}";

        Matcher matcher= Pattern.compile(pattern).matcher(strDate);
        if(!matcher.find()) {
            System.out.println("Date Not Found!");
            return "Date Not Found!";
        }
        String dateString=strDate.substring(matcher.start(),matcher.end());
        System.out.println("In line date is found: "+dateString+";");
        return dateString;
    }


}
