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
    private String Code_savoir_faire;
    private String Libelle_savoir_faire;
    private String Code_type_savoir_faire;
    private String Libelle_type_savoir_faire;

    DataBase db;
    long compteur=0;
    long compteurErr=0;
    long sum=0;
    private String DBdriver;
    private String DBlogin;
    private String DBmdp;
    private String DBconnect;
    private long debut;
    
     int MAXSIZE=1000;
      String fichierDir;
    
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

    
    public RppsImport(String[] argument) {
        
        //controler les arguments
          if (argument.length<1){help();}
       
          if (argument.length>=2){
        try{
            MAXSIZE=Integer.parseInt(argument[1],10);
        }
        catch (NumberFormatException e){MAXSIZE=1000;}
         }
         
          
        fichierDir=argument[0];
        
        System.out.println("Batch_Size= "+MAXSIZE);
        System.out.println("File_CSV= "+fichierDir);
        
        //lire le fichier de paramètrage
        lire_properties();
        
        //ouverture database
        db=new DataBase(DBdriver);
        if (db==null){System.exit(1);} //arreter si impossible...
        
        //connexion database, stop si impossible... avec un message explicite
        if (db.connect_db(DBconnect, DBlogin, DBmdp)==false){System.out.println("Connection DB impossible!!!");System.exit(2);}
        
        //valider le truncate de la table...
        YesNo();
        
        //truncate de la table avant de commencer
        String sql="TRUNCATE `rpps`.`rpps`";
        db.update(sql);
        System.out.println("Truncate table OK");
        
        //debut de la proc d'intégration
        System.out.println("DEBUT INTEGRATION...");
        try {
            debut=System.currentTimeMillis();
          
            InputStreamReader in=new InputStreamReader(new FileInputStream(fichierDir), "UTF8");
            BufferedReader bf=new BufferedReader(in);
           
            //autocomit=off & statement OK
            db.prepareBatch();
            
           //boucle d'intégration
           while (bf.ready())
           {
                ligne=bf.readLine();
                parseLine(ligne);
                compteur++;
              
                
                if ((compteur % MAXSIZE)==0)
                {
                    long[] rep=db.batchExec(MAXSIZE); //Insert et commit...
                    for (int i=0;i<MAXSIZE;i++){sum=sum+rep[i];}
                    
                    System.out.println("#"+compteur +" lignes traitées.");
                    System.out.println("@"+sum+" lignes intégrées.");
                }
           }
           
            //avant de quitter un dernier commit
           db.batchExec(MAXSIZE);
            System.out.println("Fin de l'intégration...");
            System.out.println("Nombre de lignes traitées : "+ compteur);
            System.out.println("Nombre de lignes en erreur de lecture fichier : "+compteurErr);
            System.out.println("Nombre de lignes intégrées : "+(compteur-compteurErr));
            System.out.println("Durée: "+(System.currentTimeMillis()-debut)/1000+" secondes");
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
Type_d_identifiant_PP=sc.next().replaceAll("\"", "").replaceAll("'", "''");
Identifiant_PP=sc.next().replaceAll("\"", "").replaceAll("'", "''");
Identification_nationale_PP=sc.next().replaceAll("\"", "").replaceAll("'", "''");
Code_civilite_exercice=sc.next().replaceAll("\"", "").replaceAll("'", "''");
Libelle_civilite_exercice=sc.next().replaceAll("\"", "").replaceAll("'", "''");
Nom_d_exercice=sc.next().replaceAll("\"", "").replaceAll("'", "''");
Prenom_d_exercice=sc.next().replaceAll("\"", "").replaceAll("'", "''");
Code_profession=sc.next().replaceAll("\"", "").replaceAll("'", "''");
Libelle_profession=sc.next().replaceAll("\"", "").replaceAll("'", "''");
Code_categorie_professionnelle=sc.next().replaceAll("\"", "").replaceAll("'", "''");
Libelle_categorie_professionnelle=sc.next().replaceAll("\"", "").replaceAll("'", "''");
Code_savoir_faire=sc.next().replaceAll("\"", "").replaceAll("'", "''");
Libelle_savoir_faire=sc.next().replaceAll("\"", "").replaceAll("'", "''");
Code_type_savoir_faire=sc.next().replaceAll("\"", "").replaceAll("'", "''");
Libelle_type_savoir_faire=sc.next().replaceAll("\"", "").replaceAll("'", "''");
Numero_SIRET_site=sc.next().replaceAll("\"", "").replaceAll("'", "''");
Numero_SIREN_site=sc.next().replaceAll("\"", "").replaceAll("'", "''");
Numero_FINESS_site=sc.next().replaceAll("\"", "").replaceAll("'", "''");
Numero_FINESS_etablissement_juridique=sc.next().replaceAll("\"", "").replaceAll("'", "''");
Raison_sociale_site=sc.next().replaceAll("\"", "").replaceAll("'", "''");
Enseigne_commerciale_site=sc.next().replaceAll("\"", "").replaceAll("'", "''");
Identifiant_structure=sc.next().replaceAll("\"", "").replaceAll("'", "''");
Complement_destinataire=sc.next().replaceAll("\"", "").replaceAll("'", "''");
Complement_point_geographique=sc.next().replaceAll("\"", "").replaceAll("'", "''");
Numero_Voie=sc.next().replaceAll("\"", "").replaceAll("'", "''");
Indice_repetition_voie=sc.next().replaceAll("\"", "").replaceAll("'", "''");
Code_type_de_voie=sc.next().replaceAll("\"", "").replaceAll("'", "''");
Libelle_type_de_voie=sc.next().replaceAll("\"", "").replaceAll("'", "''");
Libelle_Voie=sc.next().replaceAll("\"", "").replaceAll("'", "''");
Mention_distribution=sc.next().replaceAll("\"", "").replaceAll("'", "''");
Bureau_cedex=sc.next().replaceAll("\"", "").replaceAll("'", "''");
Code_postal=sc.next().replaceAll("\"", "").replaceAll("'", "''");
Code_commune=sc.next().replaceAll("\"", "").replaceAll("'", "''");
Libelle_commune=sc.next().replaceAll("\"", "").replaceAll("'", "''");
Code_pays=sc.next().replaceAll("\"", "").replaceAll("'", "''");
Libelle_pays=sc.next().replaceAll("\"", "").replaceAll("'", "''");
Telephone=sc.next().replaceAll("\"", "").replaceAll("'", "''");
Telephone_2=sc.next().replaceAll("\"", "").replaceAll("'", "''");
Telecopie=sc.next().replaceAll("\"", "").replaceAll("'", "''");
Adresse_e_mail=sc.next().replaceAll("\"", "").replaceAll("'", "''");
Adresse_BAL_MSSante=sc.next().replaceAll("\"", "").replaceAll("'", "''");
insert_db();
} catch (Exception e){System.out.println("erreur de lecture a la ligne : "+compteur);compteurErr++;}    
}
    }

    
    public void help(){
          System.out.println("syntaxe : rppsImport fichier_import_csv [Batch_Size]");
            System.out.println("'fichier_import_csv' à récuperer sur le site :");
            System.out.println("https://annuaire.sante.fr/web/site-pro/extractions-publiques");
            System.out.println("NB: Ne pas modifier le format du fichier...");
            System.out.println("Batch_size= [optionnel] nb update par transaction/commit valeur par defaut 1000");
            System.exit(0);
    }
    
    
    
    public static void main(String[] args) {
       
        RppsImport rppsImport = new RppsImport(args);
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
       db.updateBatch(sql);
    }

    
    /************************
     *  procedure de validation 
     * du truncate de la table
     **************************/
    private void YesNo() {
        System.out.print("L'application va réaliser un truncate de la table RPPS, voulez vous continuer (o/n)? ");
        Scanner sc=new Scanner(System.in);
        String reponse=sc.nextLine();
        if (reponse.compareToIgnoreCase("n")==0){
            System.out.println("OK, on s'arrete la sur la procédure d'import dans ce cas...");
            System.exit(1);
        }
    }
    
}
