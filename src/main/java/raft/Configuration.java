package raft;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class Configuration {
  Map<String, String> map = new HashMap();

  public Configuration()
      throws ParserConfigurationException, IOException, SAXException {
    InputStream in = Configuration.class.getClassLoader().getResourceAsStream("config.xml");
    DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
    DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
    Document doc = dBuilder.parse(in);
    doc.getDocumentElement().normalize();
    NodeList nList = doc.getElementsByTagName("property");
    for (int temp = 0; temp < nList.getLength(); temp++) {
      Node nNode = nList.item(temp);
      if (nNode.getNodeType() == Node.ELEMENT_NODE) {
        Element eElement = (Element) nNode;
        String key =
            eElement.getElementsByTagName("name").item(0).getTextContent();
        String value =
            eElement.getElementsByTagName("value").item(0).getTextContent();
        map.put(key, value);
      }
    }
    in.close();
  }

  public String get(String key) {
    return map.get(key);
  }

  public String get(String key, String defaultValue) {
    String value = map.get(key);
    if (value == null) {
      return defaultValue;
    } else {
      return value;
    }
  }

  public void set(String key, String value) {
    map.put(key, value);
  }
}
