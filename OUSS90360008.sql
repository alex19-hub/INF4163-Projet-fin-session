DROP TABLE IF EXISTS ToutesTransactions_Normalisee;
Go 

CREATE TABLE ToutesTransactions_Normalisee (
    TransactionID INT PRIMARY KEY IDENTITY(1,1),
    Payment_Method NVARCHAR(50),
    Product NVARCHAR(100),  -- Produit unique (une seule valeur)
    Prix DECIMAL(10,2),
    DateTransaction DATE,
    Discount_Applied NVARCHAR(100),
    Promotion NVARCHAR(100),
    City NVARCHAR(100),
    Store_Type NVARCHAR(100),
    Customer_category NVARCHAR(100),
    Season NVARCHAR(100),
    Customer_Name NVARCHAR(100),
    Quantite INT
);


DROP TABLE IF EXISTS Client;
Go

-- CLIENT
CREATE TABLE Client (
    ClientID INT PRIMARY KEY IDENTITY(1,1),
    NomClient NVARCHAR(100),
    CategorieClient NVARCHAR(50)
);
Go

DROP TABLE IF EXISTS Produit;
Go

-- PRODUIT
CREATE TABLE Produit (
    ProduitID INT PRIMARY KEY IDENTITY(1,1),
    NomProduit NVARCHAR(100),
    Prix DECIMAL(10,2),
    Stock INT
);
Go

DROP TABLE IF EXISTS Magasin;
Go

-- MAGASIN
CREATE TABLE Magasin (
    MagasinID INT PRIMARY KEY IDENTITY(1,1),
    TypeMagasin NVARCHAR(50),
    Ville NVARCHAR(100)
);
Go

DROP TABLE IF EXISTS Paiement;
Go
-- PAIEMENT
CREATE TABLE Paiement (
    IDPaiement INT PRIMARY KEY IDENTITY(1,1),
    MethodePaiement NVARCHAR(50)
);
Go 

DROP TABLE IF EXISTS Promotion;
Go

-- PROMOTION
CREATE TABLE Promotion (
    IDPromotion INT PRIMARY KEY IDENTITY(1,1),
    NomPromotion NVARCHAR(100),
    TypeReduction NVARCHAR(100),
    CONSTRAINT UQ_Promotion UNIQUE (NomPromotion, TypeReduction)
);
Go

DROP TABLE IF EXISTS Transactions;
Go

-- TRANSACTION
CREATE TABLE Transactions (
    TransactionID INT PRIMARY KEY IDENTITY(1,1),
    DateTransac DATETIME,
    Total DECIMAL(10,2),
    Saison NVARCHAR(50),
    ClientID INT,
    MagasinID INT,
    IDPaiement INT,
    IDPromotion INT,
    FOREIGN KEY (ClientID) REFERENCES Client(ClientID),
    FOREIGN KEY (MagasinID) REFERENCES Magasin(MagasinID),
    FOREIGN KEY (IDPaiement) REFERENCES Paiement(IDPaiement),
    FOREIGN KEY (IDPromotion) REFERENCES Promotion(IDPromotion)
);
Go


DROP TABLE IF EXISTS LigneTransaction;
Go

-- LIGNE TRANSACTION (produit acheté dans une transaction avec quantité)
CREATE TABLE LigneTransaction (
    TransactionID INT ,
    ProduitID INT,
    Quantite INT,
    PRIMARY KEY (TransactionID, ProduitID),
    FOREIGN KEY (TransactionID) REFERENCES Transactions(TransactionID),
    FOREIGN KEY (ProduitID) REFERENCES Produit(ProduitID)
);
Go


-- Declencheur 
CREATE TRIGGER trg_UpdateStock_AfterInsert_LigneTransaction
ON LigneTransaction
AFTER INSERT
AS
BEGIN
    SET NOCOUNT ON;

    -- Vérifie si une des quantités totales dépasse le stock disponible
    IF EXISTS (
        SELECT 1
        FROM (
            SELECT ProduitID, SUM(Quantite) AS TotalQuantite
            FROM INSERTED
            GROUP BY ProduitID
        ) I
        JOIN Produit P ON P.ProduitID = I.ProduitID
        WHERE P.Stock < I.TotalQuantite
    )
    BEGIN
        RAISERROR('Stock insuffisant pour au moins un produit.', 16, 1);
        ROLLBACK TRANSACTION;
        RETURN;
    END;

    -- Mise à jour du stock en fonction de la somme des quantités
    UPDATE P
    SET P.Stock = P.Stock - I.TotalQuantite
    FROM Produit P
    JOIN (
        SELECT ProduitID, SUM(Quantite) AS TotalQuantite
        FROM INSERTED
        GROUP BY ProduitID
    ) I ON P.ProduitID = I.ProduitID;
END;
GO

--Produits

INSERT INTO ToutesTransactions_Normalisee (
    Payment_Method,
    Prix,
    DateTransaction,
    Discount_Applied,
    Promotion,
    Product ,  -- Produit unique (une seule valeur)
    City ,
    Store_Type ,
    Customer_category ,
    Season ,
    Customer_Name ,
    Quantite 
)
SELECT
    TT.Payment_Method,
    TT.Total_Cost,
    TT.Date,-- ici on utilise le vrai nom de colonne
    TT.Discount_Applied,
    TT.Promotion,
    REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(value)), '[', ''), ']', ''), '''', '') AS Product,
    TT.City,
    TT.Store_Type,
    TT.Customer_Category,
    TT.Season,
    TT.Customer_Name,
    TT.Total_Items
    
FROM ToutesTransations TT
CROSS APPLY STRING_SPLIT(TT.Product, ',');


-- Clients distincts
INSERT INTO Client (NomClient, CategorieClient)
SELECT DISTINCT Customer_Name, Customer_Category
FROM ToutesTransactions_Normalisee;
Go

-- Magasins
INSERT INTO Magasin (TypeMagasin, Ville)
SELECT DISTINCT Store_Type, City
FROM ToutesTransactions_Normalisee;
Go

--Paiement
INSERT INTO Paiement (MethodePaiement)
SELECT DISTINCT Payment_Method
FROM ToutesTransactions_Normalisee
WHERE Payment_Method IS NOT NULL
    AND Payment_Method NOT IN (
        SELECT MethodePaiement FROM Paiement
    );
Go

--Promotion
INSERT INTO Promotion (NomPromotion, TypeReduction)
SELECT DISTINCT TT.Promotion, TT.Discount_Applied
FROM ToutesTransactions_Normalisee TT
LEFT JOIN Promotion P
    ON TT.Promotion = P.NomPromotion AND TT.Discount_Applied = P.TypeReduction
WHERE TT.Promotion IS NOT NULL
  AND TT.Discount_Applied IS NOT NULL
  AND P.IDPromotion IS NULL;


-- Transactions
INSERT INTO Transactions (DateTransac, Total, Saison, ClientID, MagasinID, IDPaiement, IDPromotion)
SELECT DISTINCT
    NTN.DateTransaction AS DateTransac,
    SUM(NTN.Prix) OVER (PARTITION BY NTN.TransactionID) AS Total, -- total de la transaction
    NTN.Season AS Saison,
    C.ClientID,
    M.MagasinID,
    P.IDPaiement,
    PR.IDPromotion
FROM ToutesTransactions_Normalisee NTN
JOIN Client C ON NTN.Customer_Name = C.NomClient
JOIN Magasin M ON NTN.City = M.Ville AND NTN.Store_Type = M.TypeMagasin
JOIN Paiement P ON NTN.Payment_Method = P.MethodePaiement
LEFT JOIN Promotion PR ON NTN.Promotion = PR.NomPromotion AND NTN.Discount_Applied = PR.TypeReduction;
Go

    --Produits

INSERT INTO Produit (NomProduit, Prix, Stock)
SELECT 
    NTN.Product AS NomProduit,
    MAX(NTN.Prix) AS Prix,
    (SELECT COUNT(DISTINCT Customer_Name) FROM ToutesTransactions_Normalisee) AS Stock
FROM ToutesTransactions_Normalisee NTN
WHERE NTN.Product IS NOT NULL
GROUP BY NTN.Product;
Go

--LigneTransaction
INSERT INTO LigneTransaction (TransactionID, ProduitID, Quantite)
SELECT 
    T.TransactionID,
    P.ProduitID,
    SUM(NTN.Quantite) AS Quantite
FROM ToutesTransactions_Normalisee NTN
JOIN Produit P ON NTN.Product = P.NomProduit
JOIN Client C ON NTN.Customer_Name = C.NomClient
JOIN Magasin M ON NTN.City = M.Ville AND NTN.Store_Type = M.TypeMagasin
JOIN Paiement PM ON NTN.Payment_Method = PM.MethodePaiement
LEFT JOIN Promotion PR ON NTN.Promotion = PR.NomPromotion AND NTN.Discount_Applied = PR.TypeReduction
JOIN Transactions T 
    ON T.DateTransac = NTN.DateTransaction
    AND T.ClientID = C.ClientID
    AND T.MagasinID = M.MagasinID
    AND T.IDPaiement = PM.IDPaiement
    AND (T.IDPromotion = PR.IDPromotion OR (T.IDPromotion IS NULL AND PR.IDPromotion IS NULL))
GROUP BY 
    T.TransactionID,
    P.ProduitID;
Go




