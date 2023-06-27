# Robust-E-commerce-Platform-with-Stock-and-Session-Management
This project revolves around the creation of a versatile e-commerce application that uses PostgreSQL as its database backbone. It aims to provide a platform that bridges sellers with their prospective customers, akin to prevalent platforms like Amazon and Trendyol.

The project is primarily divided into two parts:

Database Creation and Data Import: This portion entails the construction of the database schema and the subsequent importation of relevant data.
Application Development: This segment involves the development of an application interface that facilitates seamless interaction between sellers and buyers.

There are three core restrictions or business rules that the application needs to enforce to ensure smooth operations:

Session Limitation: Each seller will have a designated maximum number of simultaneous sessions based on their subscription plan. Once a seller reaches their session limit, further sign-in attempts will be denied until some of the active sessions are logged out.
Stock Limitation for Sellers: Every seller has a maximum stock per product limit, dictated by their chosen subscription plan. When a seller's product inventory hits this limit, subsequent stock update requests will be denied. The stock quantity should always remain within the 0 to maximum limit range as defined by the seller's plan.
Stock Limitation for Buyers: The application also enforces the stock limitation during the customer's shopping process. When a customer attempts to add items to their cart or proceed to checkout, the application should validate the items against the seller's available stock.
