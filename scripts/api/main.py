from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import routes 

app = FastAPI(
    title="Urban Data Explorer API",
    description="API REST pour servir les données agrégées du marché immobilier parisien."
)

# Configuration CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Inclure le routeur contenant tous les endpoints
app.include_router(routes.router)

@app.get("/", tags=["Root"])
async def root():
    """ Endpoint d'accueil simple. """
    return {"message": "Bienvenue sur l'Urban Data Explorer API. Consultez /docs pour la documentation."}