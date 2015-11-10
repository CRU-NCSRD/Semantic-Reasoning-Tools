/*
 * Copyright (C) 2015 Computational Systems & Human Mind Research Unit
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package gr.demokritos.iit.cru.creativity.reasoning.semantic;

import gr.demokritos.iit.cru.creativity.utilities.Connect;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

/**
 *
 * @author George Panagopoulos
 */
public class ThinkingSeedGenerator {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException, Exception {
        System.out.println(ThinkingSeedGenerator("friend", 2, "en"));
    }

    public static String ThinkingSeedGenerator(String seed, int difficulty, String language) throws ClassNotFoundException, SQLException, IOException, InstantiationException, IllegalAccessException {

        ArrayList<String> mined = new ArrayList<String>();
        Connect c = new Connect(language);
        RandomWordGenerator r = new RandomWordGenerator(c);
        String randomPhrase = r.selectRandomWord(seed, difficulty);

        int size = randomPhrase.split(",").length;

        Random rand = new Random();
        if (language.equalsIgnoreCase("en")) {
            while (mined.size() < size) {
                int Point = rand.nextInt(1172);//number of words in english thesaurus
                String word = FileUtils.readLines(new File(c.getEnglish_thes())).get(Point).trim();
                if (mined.contains(word)) {//|| inf.getStop().contains(word)) {
                    continue;
                }
                mined.add(word);
            }
        } else if (language.equalsIgnoreCase("de")) {
            while (mined.size() < size) {
                int Point = rand.nextInt(1704);//number of words in german thesaurus
                String word = FileUtils.readLines(new File(c.getGerman_thes())).get(Point).trim();
                if (mined.contains(word)) {
                    continue;
                }
                mined.add(word);
            }
        } else {
            while (mined.size() < size) {
                int Point = rand.nextInt(933);//number of words in greek thesaurus
                String word = FileUtils.readLines(new File(c.getGreek_thes())).get(Point).trim();
                if (mined.contains(word)) {
                    continue;
                }
                mined.add(word);
            }
        }
        c.CloseConnection();

        return StringUtils.join(mined, ",");
    }
}
