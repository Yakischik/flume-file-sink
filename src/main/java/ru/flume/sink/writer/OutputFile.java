package ru.flume.sink.writer;

import java.io.File;

/**
 * Класс для управления именами файлов, использующихся при записи логов. Позволяет генерировать имена для
 * временных и финальных файлов.
 */
public class OutputFile {

    private static final String TEMP_EXTENSION = ".tmp";
    
    private File parent;
    private String name;
    
    private long ts = System.currentTimeMillis();

//----------------------------------------//    
    /**
     * Создает генератор имен файла по указанным параметрам. Файл будет храниться в корневой папке с указанным
     * относительным путём (т.е. в качестве имени файла можно передать не только "myFile", но и "my/files/myFile").
     * Расширение файла указывать не требуется, оно присваивается автоматически.
     *
     * @param root корневая папка для записи файлов
     * @param fileName относительный путь к файлу
     */
    public OutputFile(File root, String fileName) {
        fileName = fileName.replace("\\", "/");
        
        // если по какой то причине указан абсолютный путь - считаем это опечаткой, чтобы не выходить за пределы
        // папки, указанной в конфигурации
        if (fileName.startsWith("/")) {
            fileName = fileName.substring(1);
        }

        // если по каким-то причинам имя оканчивается на слеш (по сути, ошибка на стороне клиента, 
        // генерирующего имя файла), то считаем, что последняя часть пути есть имя файла
        if (fileName.endsWith("/")) {
            fileName = fileName.substring(0, fileName.length() - 1);
        }
        
        // если в имени файла присутствует вложенность, необходимо построить полный путь до папки
        int separator = fileName.lastIndexOf("/");
        if (separator >= 0) {
            String subDir = fileName.substring(0, separator);            
            String rootDir = root.toString();
            if (!rootDir.endsWith("/")) {
                rootDir = rootDir + "/";
            }
            this.parent = new File(rootDir + subDir);
            this.name = fileName.substring(separator + 1); 
        } else {
            this.parent = root;
            this.name = fileName;
        }      
    }

//----------------------------------------//
    /**
     * @return папку, в которой будет размещен файл
     */
    public File getParent() {
        return parent;
    }

//----------------------------------------//
    /**
     * @return имя файла (только имя, без папок)
     */
    public String getName() {
        return name;
    }
    
//----------------------------------------//
    /**
     * @return полный путь к результирующему файлу (если файл уже существует, к имени добавится индекс)
     */
    public File getOutput(String extension) {
        int i = 1;
        File output = new File(parent, name + extension);
        while (output.exists()) {            
            output = new File(parent, name + " (" + i + ")" + extension);
            i++;
        }
        return output;
    }
//----------------------------------------//
    /**
     * @return временный файл для записи
     */
    public File getTemp() {
        return new File(parent, name + "." + ts + TEMP_EXTENSION);
    }
}
