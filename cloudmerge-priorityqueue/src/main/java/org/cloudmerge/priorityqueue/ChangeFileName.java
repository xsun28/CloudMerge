package org.cloudmerge.priorityqueue;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ChangeFileName {
	private Path dir;
	
	public ChangeFileName(String input){
		dir = Paths.get(input);	
	}
	
	public void changeNames() throws IOException{
		Pattern pattern = Pattern.compile("[1-9]+[\\d]*.bz2$");
		try(DirectoryStream<Path> files = Files.newDirectoryStream(dir, "*.bz2")){
			for(Path file : files){
				String fileName = file.getFileName().toString();
				Matcher matcher = pattern.matcher(fileName);
				if(!matcher.find()) continue;
				Path newName = Paths.get(fileName.substring(matcher.start()));
				newName = dir.resolve(newName);
				Files.move(file,newName,StandardCopyOption.REPLACE_EXISTING);
			}
		}
	}
	public static void main(String args[]) throws IOException{
		ChangeFileName change = new ChangeFileName(args[0]);
		change.changeNames();
	}
}
