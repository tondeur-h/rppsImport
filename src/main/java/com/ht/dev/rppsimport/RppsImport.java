package com.ht.dev.rppsimport;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author tondeur-h
 */
public class RppsImport 
{
String ligne;
    private String Type_d_identifiant_PP;
    private String Identifiant_PP;
    private String Identification_nationale_PP;
    private String Code_civilite_exercice;
    private String Libelle_civilite_exercice;
    private String Nom_d_exercice;
    private String Prenom_d_exercice;
    private String Code_profession;
    private String Libelle_profession;
    private String Code_categorie_professionnelle;
    private String Libelle_categorie_professionnelle;
    private String Code_savoir;
    private String Adresse_BAL_MSSante;
    private String Adresse_e_mail;
    private String Telecopie;
    private String Telephone_2;
    private String Telephone;
    private String Libelle_pays;
    private String Code_pays;
    private String Libelle_commune;
    private String Code_commune;
    private String Code_postal;
    private String Bureau_cedex;
    private String Mention_distribution;
    private String Libelle_Voie;
    private String Libelle_type_de_voie;
    private String Code_type_de_voie;
    private String Indice_repetition_voie;
    private String Numero_Voie;
    private String Complement_point_geographique;
    private String Complement_destinataire;
    private String Identifiant_structure;
    private String Enseigne_commerciale_site;
    private String Raison_sociale_site;
    private String Numero_FINESS_etablissement_juridique;
    private String Numero_FINESS_site;
    private String Numero_SIREN_site;
    private String Numero_SIRET_site;
    private String Libelle_type_savoir;
    private String Code_savoir_faire;
    private String Libelle_savoir_faire;
    private String Code_type_savoir_faire;
    private String Libelle_type_savoir_faire;

    DataBase db;
    long compteur=0;
    private String DBdriver;
    private String DBlogin;
    private String DBmdp;
    private String DBconnect;
    
    
    /**
     * **********************************************
     * Lire le fichier des propriétés du connecteur
     ***********************************************
     */
    private void lire_properties() {
        //lire les chemins par defaut dans un fichier de propriété
        try {
            //chemin de l'application
            String cheminAPP = System.getProperty("user.dir");
            //lecture du fichier propriété
            Properties pp = new Properties();

            pp.load(new FileReader(cheminAPP + "/rppsimport.properties"));
            
            DBconnect = pp.getProperty("dbconnect", "jdbc:mariadb://localhost:3306/RPPS");  //url jdbc
            DBlogin = pp.getProperty("dblogin", "root");
            DBmdp = pp.getProperty("dbpassword", "admin");
            DBdriver = pp.getProperty("dbdriver", "org.mariadb.jdbc.Driver");
            
            System.out.println("dbconnect="+DBconnect);
            System.out.println("dblogin="+DBlogin);
            System.out.println("dbpassword="+DBmdp);
            System.out.println("dbdriver"+DBdriver);
            
        } catch (IOException e) {
            System.out.println("Le fichier rppsimport.properties est manquant...\nMerci de creer ce fichier et de le mettre dans le répertoire de mon jar\n");
            System.exit(1);
        }
    }

    
    public RppsImport(String fileDir) {
        //lire le fichier de paramètrage
        lire_properties();
        
        db=new DataBase(DBdriver);
        if (db==null){System.exit(1);}
        
        if (db.connect_db(DBconnect, DBlogin, DBmdp)==false){System.out.println("Connection DB impossible!!!");System.exit(2);}
        
        YesNo();
        
        //truncate de la table avant de commencer
        String sql="TRUNCATE `rpps`.`rpps`";
        db.update(sql);
        System.out.println("Truncate table OK");
        
        System.out.println("DEBUT INTEGRATION...");
        try {
            //Reader in = new FileReader("C:\\Users\\tondeur-h\\Downloads\\ExtractionMonoTable_CAT18_ToutePopulation_201707030951/ExtractionMonoTable_CAT18_ToutePopulation_201707030751.csv");
          
            InputStreamReader in=new InputStreamReader(new FileInputStream(fileDir), "UTF8");
            BufferedReader bf=new BufferedReader(in);
           
           while (bf.ready()){
           ligne=bf.readLine();
           parseLine(ligne);
           compteur++;
               //System.out.println(compteur);
           if ((compteur % 1000)==0){
               System.out.println("#"+compteur +" lignes");
           }
           }
            System.out.println("Fin de l'intégration...");
            System.out.println("Nombre de lignes : "+ compteur);
                   } catch (IOException ex) {
            Logger.getLogger(RppsImport.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    
    public void parseLine(String line){
        Scanner sc=new Scanner(line);
        sc.useDelimiter(";");
        //System.out.println(line);
        while (sc.hasNext()){
try{
Type_d_identifiant_PP=sc.next().replaceAll("\"", "");
Identifiant_PP=sc.next().replaceAll("\"", "");
Identification_nationale_PP=sc.next().replaceAll("\"", "");
Code_civilite_exercice=sc.next().replaceAll("\"", "");
Libelle_civilite_exercice=sc.next().replaceAll("\"", "");
Nom_d_exercice=sc.next().replaceAll("\"", "");
Prenom_d_exercice=sc.next().replaceAll("\"", "");
Code_profession=sc.next().replaceAll("\"", "");
Libelle_profession=sc.next().replaceAll("\"", "");
Code_categorie_professionnelle=sc.next().replaceAll("\"", "");
Libelle_categorie_professionnelle=sc.next().replaceAll("\"", "");
Code_savoir_faire=sc.next().replaceAll("\"", "");
Libelle_savoir_faire=sc.next().replaceAll("\"", "");
Code_type_savoir_faire=sc.next().replaceAll("\"", "");
Libelle_type_savoir_faire=sc.next().replaceAll("\"", "");
Numero_SIRET_site=sc.next().replaceAll("\"", "");
Numero_SIREN_site=sc.next().replaceAll("\"", "");
Numero_FINESS_site=sc.next().replaceAll("\"", "");
Numero_FINESS_etablissement_juridique=sc.next().replaceAll("\"", "");
Raison_sociale_site=sc.next().replaceAll("\"", "");
Enseigne_commerciale_site=sc.next().replaceAll("\"", "");
Identifiant_structure=sc.next().replaceAll("\"", "");
Complement_destinataire=sc.next().replaceAll("\"", "");
Complement_point_geographique=sc.next().replaceAll("\"", "");
Numero_Voie=sc.next().replaceAll("\"", "");
Indice_repetition_voie=sc.next().replaceAll("\"", "");
Code_type_de_voie=sc.next().replaceAll("\"", "");
Libelle_type_de_voie=sc.next().replaceAll("\"", "");
Libelle_Voie=sc.next().replaceAll("\"", "");
Mention_distribution=sc.next().replaceAll("\"", "");
Bureau_cedex=sc.next().replaceAll("\"", "");
Code_postal=sc.next().replaceAll("\"", "");
Code_commune=sc.next().replaceAll("\"", "");
Libelle_commune=sc.next().replaceAll("\"", "");
Code_pays=sc.next().replaceAll("\"", "");
Libelle_pays=sc.next().replaceAll("\"", "");
Telephone=sc.next().replaceAll("\"", "");
Telephone_2=sc.next().replaceAll("\"", "");
Telecopie=sc.next().replaceAll("\"", "");
Adresse_e_mail=sc.next().replaceAll("\"", "");
Adresse_BAL_MSSante=sc.next().replaceAll("\"", "");
insert_db();
} catch (Exception e){System.out.println("erreur a la ligne : "+compteur);}    
}
    }
    
    public static void main(String[] args) {
        if (args.length<1){
            System.out.println("syntaxe : rppsImport fichier_import_csv");
            System.out.println("'fichier_import_csv' à récuperer sur le site :");
            System.out.println("https://annuaire.sante.fr/web/site-pro/extractions-publiques");
            System.out.println("NB: Ne pas modifier le format du fichier...");
            System.exit(0);
        }
       
        RppsImport rppsImport = new RppsImport(args[0]);
    }

    private void insert_db() {
        String sql="INSERT INTO `rpps`.`rpps`\n" +
"(`Type_d_identifiant_PP`,\n" +
"`Identifiant_PP`,\n" +
"`Identification_nationale_PP`,\n" +
"`Code_civilite_exercice`,\n" +
"`Libelle_civilite_exercice`,\n" +
"`Nom_d_exercice`,\n" +
"`Prenom_d_exercice`,\n" +
"`Code_profession`,\n" +
"`Libelle_profession`,\n" +
"`Code_categorie_professionnelle`,\n" +
"`Libelle_categorie_professionnelle`,\n" +
"`Code_savoir_faire`,\n" +
"`Libelle_savoir_faire`,\n" +
"`Code_type_savoir_faire`,\n" +
"`Libelle_type_savoir_faire`,\n" +
"`Numero_SIRET_site`,\n" +
"`Numero_SIREN_site`,\n" +
"`Numero_FINESS_site`,\n" +
"`Numero_FINESS_etablissement_juridique`,\n" +
"`Raison_sociale_site`,\n" +
"`Enseigne_commerciale_site`,\n" +
"`Identifiant_structure`,\n" +
"`Complement_destinataire`,\n" +
"`Complement_point_geographique`,\n" +
"`Numero_Voie`,\n" +
"`Indice_repetition_voie`,\n" +
"`Code_type_de_voie`,\n" +
"`Libelle_type_de_voie`,\n" +
"`Libelle_Voie`,\n" +
"`Mention_distribution`,\n" +
"`Bureau_cedex`,\n" +
"`Code_postal`,\n" +
"`Code_commune`,\n" +
"`Libelle_commune`,\n" +
"`Code_pays`,\n" +
"`Libelle_pays`,\n" +
"`Telephone`,\n" +
"`Telephone_2`,\n" +
"`Telecopie`,\n" +
"`Adresse_e_mail`,\n" +
"`Adresse_BAL_MSSante`)\n" +
"VALUES\n" +
"('"+Type_d_identifiant_PP+"',\n" +
"'"+Identifiant_PP+"',\n" +
"'"+Identification_nationale_PP+"',\n" +
"'"+Code_civilite_exercice+"',\n" +
"'"+Libelle_civilite_exercice+"',\n" +
"'"+Nom_d_exercice+"',\n" +
"'"+Prenom_d_exercice+"',\n" +
"'"+Code_profession+"',\n" +
"'"+Libelle_profession+"',\n" +
"'"+Code_categorie_professionnelle+"',\n" +
"'"+Libelle_categorie_professionnelle+"',\n" +
"'"+Code_savoir_faire+"',\n" +
"'"+Libelle_savoir_faire+"',\n" +
"'"+Code_type_savoir_faire+"',\n" +
"'"+Libelle_type_savoir_faire+"',\n" +
"'"+Numero_SIRET_site+"',\n" +
"'"+Numero_SIREN_site+"',\n" +
"'"+Numero_FINESS_site+"',\n" +
"'"+Numero_FINESS_etablissement_juridique+"',\n" +
"'"+Raison_sociale_site+"',\n" +
"'"+Enseigne_commerciale_site+"',\n" +
"'"+Identifiant_structure+"',\n" +
"'"+Complement_destinataire+"',\n" +
"'"+Complement_point_geographique+"',\n" +
"'"+Numero_Voie+"',\n" +
"'"+Indice_repetition_voie+"',\n" +
"'"+Code_type_de_voie+"',\n" +
"'"+Libelle_type_de_voie+"',\n" +
"'"+Libelle_Voie+"',\n" +
"'"+Mention_distribution+"',\n" +
"'"+Bureau_cedex+"',\n" +
"'"+Code_postal+"',\n" +
"'"+Code_commune+"',\n" +
"'"+Libelle_commune+"',\n" +
"'"+Code_pays+"',\n" +
"'"+Libelle_pays+"',\n" +
"'"+Telephone+"',\n" +
"'"+Telephone_2+"',\n" +
"'"+Telecopie+"',\n" +
"'"+Adresse_e_mail+"',\n" +
"'"+Adresse_BAL_MSSante+"')";
       db.update(sql);
    }

    private void YesNo() {
        System.out.print("L'application va réaliser un truncate de la table RSS, voulez vous continuer (o/n)? ");
        Scanner sc=new Scanner(System.in);
        String reponse=sc.nextLine();
        if (reponse.compareToIgnoreCase("n")==0){
            System.exit(1);
        }
    }
    
}
