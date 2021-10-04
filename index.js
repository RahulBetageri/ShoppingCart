const dotenv = require("dotenv");
dotenv.config();
const { request, GraphQLClient } = require("graphql-request");
const { createBatchResolver } = require("graphql-resolve-batch");
const express = require("express");
const { ApolloServer, gql } = require("apollo-server-express");

const moment = require("moment");

const { Pool, Client } = require("pg");

const Minio = require("minio");
var minioClient = new Minio.Client({
  endPoint: process.env.MinioEndPoint,
  port: 443,
  useSSL: true,
  accessKey: process.env.MinioAccessKey,
  secretKey: process.env.MinioSecretKey,
});

const pool = new Pool({
  user: process.env.TIMESCALE_USER,
  host: process.env.TIMESCALE_URL,
  database: process.env.TIMESCALE_DATABASE,
  password: process.env.TIMESCALE_PASSWORD,
  port: process.env.TIMESCALE_PORT,
});

const typeDefs = gql`
  type Query {
    getAllShoppingCartDetails: [ShoppingcartData]
  }

  type Mutation {
    insertShoppingCartData(eventName: String!, dataObj: JSON!): Boolean
  }

  type ShoppingcartData {
    id: Int
    name: String
    imageUrl: String
    timestamp: String
  }
`;

const resolvers = {
  Query: {
    getAllShoppingCartDetails: (root, args, context) => {
      return new Promise(async (resolve, reject) => {
        const getShoppingCartData = {
          name: "get-cart-data",
          text: "SELECT * from shopping_cart_details order by ts ASC;",
          rowMode: "array",
        };
        let queryArgs = [];

        // queryArgs.push(args.uniqueId);

        getShoppingCartData.values = queryArgs;

        console.log("getShoppingCartData", JSON.stringify(getShoppingCartData));

        pool.query(getShoppingCartData, (err, rslt) => {
          console.log("rslt", rslt);
          if (err) {
            console.error("Postgres Query Error", err.toString());
            reject(err);
          } else if (rslt.rows.length) {
            console.log("rslt.rows", rslt.rows);
            let points = [];
            let data = rslt.rows;

            for (let i = 0; i < data.length; i++) {
              let fileName = data[i].fileName;
              let bucketName = "abc";
              let fileDetail = await getMinioFileURL(bucketName, fileName);
              let imageName = data[i].imageName;
              let imageId = data[i].id;
              let timestamp = data[i].createdAt;
              let obj = {
                id: imageId,
                name: imageName,
                imageUrl: fileDetail.url,
                timestamp: timestamp,
              };
              points.push(obj);
            }
            // rslt.rows.forEach((e) => {
            //   //push the data and resolved
            // });
            resolve(points);
          } else {
            resolve([]);
          }
        });
      });
    },
  },
  Mutation: {
    insertShoppingCartData: (root, args) => {
      return new Promise(async (resolve, reject) => {
        const currentTimestamp = Math.floor(Date.now() / 1000);
        let time = moment
          .unix(currentTimestamp)
          .utc()
          .format("YYYY-MM-DD HH:mm:ss");
        let name = args.name;
        let fileName = args.fileName;
        //postgrese insert data
        let row = [];
        const insertCartQuery = {
          // give the query a unique name
          name: "insert-cart-data",
          text: "insert into shopping_cart_details (name, fileName, createdAt) values ($1, $2, $3)",
        };

        row.push(dataObj.tsp_id);
        row.push(JSON.stringify(dataObj));
        row.push(res);
        row.push(time);

        insertCartQuery.values = row;

        console.log("parsed: ", JSON.stringify(row));
        console.log(insertCartQuery);

        (async () => {
          const { rows } = await pool.query(insertCartQuery);
          console.log("inserted:", rows);
          resolve(true);
        })().catch((e) =>
          setImmediate(() => {
            // throw e;
            console.log(insertIpQuery);
            console.error("TimeScale Insert Error: ", e.toString());
            reject(false);
          })
        );
      });
    },
  },
};

async function getMinioFileURL(bucketName, fileName) {
  if (bucketName === null || bucketName === undefined) {
    throw new Error("Please pass bucketName");
  }

  if (fileName === null || fileName === undefined) {
    throw new Error("Please pass fileName");
  }

  let minioFileURL = await getMinioFileDetail(bucketName, fileName);

  let fileDetail = {
    url: minioFileURL,
  };

  return fileDetail;
}

async function getMinioFileDetail(bucketName, fileName) {
  let urlDetail = await minioClient.presignedGetObject(
    bucketName,
    fileName,
    24 * 60 * 60
  );
  if (!urlDetail) {
    throw new Error("Minio URL not found");
  }
  return urlDetail;
}

const server = new ApolloServer({
  typeDefs,
  resolvers,
  context: ({ req, connection }) => {
    if (req && req.headers) {
      // if a subscription comes, req is null
      return req.headers;
    }

    if (connection && connection.context) {
      return connection.context;
    }
  },
  // Keep this formatError till we solve [object object] situation
  formatError: (error) => {
    console.error(error);
    throw error;
  },
});

const app = express();
server.applyMiddleware({
  app: app,
  path: "/",
  bodyParserConfig: {
    extended: true,
    limit: "50mb",
  },
});

const appServer = app.listen(process.env.SERVER_PORT, () =>
  //const appServer = app.listen(8005, () =>
  console.log(
    `🚀  Playground is now running on http://localhost:${process.env.SERVER_PORT}`
  )
);

appServer.timeout = 0;
