import asyncio
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler
from bs4 import BeautifulSoup
import aiofiles
import json
import aiohttp
from datetime import datetime
import tarfile
import io
from dotenv import load_dotenv
import os

ALLOWED_USER_ID = []
STARTED_PARS = []
ADMINS = [6557065907]

class ParseLogger:
    def __init__(self, id):
        self.log = list()
        self.id = id

    def info(self, action, message):
        date = datetime.now()
        self.log.append(f"[{date.strftime('%Y-%m-%d %H-%M-%S.') + str(date.microsecond // 1000)}] [{self.id}] [{action}/INFO] {message}")

    def error(self, action, message):
        date = datetime.now()
        self.log.append(f"[{date.strftime('%Y-%m-%d %H-%M-%S.') + str(date.microsecond // 1000)}] [{self.id}] [{action}/ERROR] {message}")

    def get_file(self):
        date = datetime.now()
        file = io.BytesIO('\n'.join(self.log).encode("utf-8"))
        file.name = f"{date.strftime('%Y-%m-%d %H-%M-%S.') + str(date.microsecond // 1000)}-{self.id}.log"
        file.seek(0)
        return file

    async def save(self):
        file = self.get_file()
        def compress():
            date = datetime.now()
            with tarfile.open(f"logs\\{date.strftime('%Y-%m-%d %H-%M-%S.') + str(date.microsecond // 1000)}-{self.id}.tar.gz", mode="w:gz") as tar:
                tarinfo = tarfile.TarInfo(name=f"{date.strftime('%Y-%m-%d %H-%M-%S.') + str(date.microsecond // 1000)}-{self.id}.log")
                tarinfo.size = len(file.getvalue())
                tar.addfile(tarinfo, file)
        await asyncio.to_thread(compress)


async def parse_profile(Logger, card, maxlcards, minregdate, maxrating):
    Logger.info('PROFILEPARSE', f"Started parse profile")
    async with aiohttp.ClientSession() as session:
        async with session.get(f"https://www.olx.kz/api/v1/offers/{card[0]}/") as r:
            Logger.info('REQUEST', f"CARDJSON https://www.olx.kz/api/v1/offers/{card[0]}/ {r.status}")
            offer = await r.json()
        async with session.get(f"https://www.olx.kz/api/v1/users/{offer['data']['user']['id']}/") as r:
            Logger.info('REQUEST', f"USER https://www.olx.kz/api/v1/users/{offer['data']['user']['id']}/ {r.status}")
            user = await r.json()
        async with session.get(f"https://www.olx.kz/api/v1/offers/?user_id={user['data']['id']}") as r:
            Logger.info('REQUEST', f"OFFERSC https://www.olx.kz/api/v1/offers/?user_id={user['data']['id']} {r.status}")
            data = await r.json()
            offers = len(data['data'])
        async with session.get(f"https://khonor.eu-sharedservices.olxcdn.com/api/olx/kz/user/{user['data']['id']}/score/rating", headers={"origin": "https://www.olx.kz"}) as r:
            Logger.info('REQUEST', f"RATING https://khonor.eu-sharedservices.olxcdn.com/api/olx/kz/user/{user['data']['id']}/score/rating {r.status} Response: {await r.text()}".replace('\n', ''))
            rating = await r.json()
        async with session.get(f"https://www.olx.kz/api/v1/offers/{str(card[0])}/limited-phones/") as r:
            Logger.info('REQUEST', f"PHONE https://www.olx.kz/api/v1/offers/{str(card[0])}/limited-phones/ {r.status} Response: {await r.text()}".replace('\n', ''))
            data = await r.json()
            if r.status != 400:
                if data['data']['phones'][0].replace(' ','').startswith("8") or data['data']['phones'][0].replace(' ','').startswith("+8"):
                    phone = f"+7{data['data']['phones'][0].replace(' ','')[1:]}"
                else:
                    phone = data['data']['phones'][0].replace(' ','')
            else:
                phone = "Не удалось получить"

    if offers > maxlcards:
        return None
    regdate = int(user['data']['created'].split("-")[0])
    if regdate <= minregdate:
        return None
    if rating['body'][0]['data']['ratings'] > maxrating:
        return None
    ncard = []
    for a in card:
        ncard.append(a)
    ncard.append(offers)
    ncard.append(regdate)
    ncard.append(rating['body'][0]['data']['ratings'])
    ncard.append(phone)
    Logger.info('PROFILEPARSE', f"Profile parsed")
    Logger.info('PROFILEPARSE', f"Card after profile parse: {ncard}")
    return ncard


async def parse_card(Logger, card, maxviewers):
    Logger.info('CARDPARSE', f"Started parse card {card[0]}")
    async with aiohttp.ClientSession() as session:
        async with session.get(card[1]) as r:
            Logger.info('REQUEST', f"CARDHTML {card[1]} {r.status}")
            response = await r.text()
        async with session.post("https://production-graphql.eu-sharedservices.olxcdn.com/graphql", headers={"Content-Type": "application/json; charset=utf-8",
                            "Authorization": "ANONYMOUS",
                            "site": "olxkz"},
                        json={"operationName": "PageViews", "variables": {"adId": str(card[0])},
                            "query": "query PageViews($adId: String!) {\n  myAds {\n    pageViews(adId: $adId) {\n      pageViews\n    }\n  }\n}"}) as r:
            Logger.info('REQUEST', f"VIEWS https://production-graphql.eu-sharedservices.olxcdn.com/graphql {r.status} Response: {await r.text()}".replace('\n', ''))
            views = await r.json()

    soup = BeautifulSoup(response, 'html.parser')

    if views['data']['myAds']['pageViews']['pageViews'] > maxviewers:
        return None
    try:
        description = soup.find(attrs={"data-testid": "ad_description"}).find("div", class_="css-1o924a9").get_text()
    except:
        description = "Нету"
    author_link = "https://www.olx.kz" + soup.find(attrs={"data-testid": "user-profile-link"})['href']
    ncard = []
    for a in card:
        ncard.append(a)
    ncard.append(views['data']['myAds']['pageViews']['pageViews'])
    ncard.append(description)
    ncard.append(author_link)
    Logger.info('CARDPARSE', f"Parsed card {ncard[0]}")
    return ncard


async def parse_list(Logger, category, pages, minprice, maxprice):
    cards = []
    for page in range(1, pages + 1):
        async with aiohttp.ClientSession() as session:
            async with session.get(f"https://www.olx.kz/{category}/?page={page}") as r:
                Logger.info('REQUEST', f"PAGE https://www.olx.kz/{category}/?page={page} {r.status}")
                html = await r.text()
        soup = BeautifulSoup(html, 'html.parser')
        l_cards = soup.find_all(attrs={"data-testid": "l-card"})
        for card in l_cards:
            Logger.info('LISTPARSE', f"Started parse card {card['id']}")
            rcard = []
            rcard.append(card['id'])
            link = card.find("a", class_="css-qo0cxu", href=True)
            if link:
                rcard.append("https://www.olx.kz" + link["href"])
            name = card.find("h4", class_="css-1sq4ur2")
            if name:
                rcard.append(name.get_text())

            price = card.find("p", attrs={"data-testid": "ad-price"})
            if price:
                rcard.append(price.get_text())

            location_date = card.find("p", attrs={"data-testid": "location-date"})
            if location_date:
                rcard.append(location_date.get_text().split(" - ")[0])
                rcard.append(location_date.get_text().split(" - ")[1])
            if price:
                if "Обмен" not in price.get_text():
                    if "Бесплатно" not in price.get_text():
                        if float(price.get_text().split(" тг")[0].replace(" ", "")) >= minprice and float(
                                price.get_text().split(" тг")[0].replace(" ", "")) <= maxprice:
                            Logger.info('LISTPARSE', f"Parsed card {card['id']}")
                            cards.append(rcard)
    Logger.info('LISTPARSE', f'Found {len(cards)} cards')
    return [x for i, x in enumerate(cards) if x[0] not in {y[0] for y in cards[:i]}]

def check_user_access(func):
    async def wrapper(update: Update, context):
        if update.effective_user.id not in ALLOWED_USER_ID:
            await update.message.reply_text("для доступа к парсеру писать @sv1zx")
            return
        await func(update, context)
    return wrapper

def check_admin(func):
    async def wrapper(update: Update, context):
        if update.effective_user.id not in ADMINS:
            await update.message.reply_text("Отказано")
            return
        await func(update, context)
    return wrapper

@check_user_access
async def start(update: Update, context):
    presets = ""
    data = dict()
    async with aiofiles.open('presets.json', 'r', encoding='utf-8') as f:
        try:
            data = json.loads(await f.read())
        except:
            data.update({"presets": []})
        for preset in data['presets']:
            if preset['id'] == update.effective_user.id:
                presets = presets + f"{preset['name']}: {preset['preset']}\n"

    await update.message.reply_text(f"/parse (Количество страниц) (Минимальная цена) (Максимальная цена) (Максимальное количество просмотров) (Максимальное количество объявлений) (Минимальная дата регистрации (год)) (Максимальный рейтинг)\n\n/parse preset (Название пресета)\n\n/preset (Название) (Количество страниц) (Минимальная цена) (Максимальная цена) (Максимальное количество просмотров) (Максимальное количество объявлений) (Минимальная дата регистрации (год)) (Максимальный рейтинг)\n\n/stop - Остановить парсер\n\nВаши пресеты:\n{presets[:-1] if presets != '' else 'У вас нету пресетов'}\n\n❤️ olx.kz neoparser by @sv1zx ❤️")

async def parse(update, context, Logger):
    try:
        global STARTED_PARS
        if update.effective_user.id in STARTED_PARS:
            await update.message.reply_text("Вы уже запустили парсер")
            return
        Logger.info('PARSE', "Parse started")
        if context.args[0] == "preset":
            async with aiofiles.open('presets.json', 'r', encoding='utf-8') as f:
                data = json.loads(await f.read())
                if any(d["id"] == update.effective_user.id and d["name"] == context.args[1] for d in data['presets']):
                    preset = next((d for d in data['presets'] if
                                   d["id"] == update.effective_user.id and d["name"] == context.args[1]), None)
                    if preset:
                        context.args = preset['preset'].split(" ")
                else:
                    await update.message.reply_text("Такого пресета не существует")
                    return
        Logger.info('PARSE', f'Used configuration: Pages - {context.args[0]} Min Price - {context.args[1]} Max Price - {context.args[2]} Max Views - {context.args[3]} Max Cards - {context.args[4]} Min Regdate - {context.args[5]} Max Rating - {context.args[6]}')
        STARTED_PARS.append(update.effective_user.id)
        found = 0
        await update.message.reply_text("Ищу объявления...")
        Logger.info('PARSE', 'LISTPARSE Started')
        cards = await parse_list(Logger, "list", int(context.args[0]), int(context.args[1]), int(context.args[2]))
        Logger.info('PARSE', f'After removing duplicates: {len(cards)}')
        if update.effective_user.id not in STARTED_PARS:
            await update.message.reply_text("Парсер остановлен")
            return
        await update.message.reply_text(
            f'Найдено объявлений по запросу ({context.args[0]} страниц, {context.args[1]} минимальная цена, {context.args[2]} максимальная цена): {len(cards)}')
        await update.message.reply_text("Начинаю парсить...")
        if update.effective_user.id not in STARTED_PARS:
            await update.message.reply_text("Парсер остановлен")
            return
        Logger.info('PARSE', 'Started cards parse')
        for card in cards:
            if update.effective_user.id not in STARTED_PARS:
                await update.message.reply_text("Парсер остановлен")
                return
            try:
                ncard = await parse_card(Logger, card, int(context.args[3]))
                if ncard:
                    pcard = await parse_profile(Logger, ncard, int(context.args[4]), int(context.args[5]), int(context.args[6]))
                    if pcard:
                        found = found + 1
                        Logger.info('PARSE', f'Found new card {pcard[0]}')
                        await update.message.reply_markdown(f"""
🆔 ID: `{pcard[0]}`
🗂 Название: `{pcard[2]}`
💰 Цена: `{pcard[3]}`
🔗 Ссылка на объявление: {pcard[1]}
📍 Местоположение: `{pcard[4]}`
👀 Просмотры: `{pcard[6]}`
ℹ️ Описание: `{pcard[7]}`
    
👤 Продавец: {pcard[8]}
⭐️ Рейтинг: `{pcard[11]}`
📞 Номер: {pcard[12]}
💬 WhatsApp: {'https://wa.me/' + pcard[12] if pcard[12] != 'Не удалось получить' else 'Не удалось получить'}
    
📅 Дата публикации: `{pcard[5]}`
📦 Кол-во объявлений: `{pcard[9]}`
📅 Дата регистрации: `{pcard[10]}`
    
❤️ olx.kz neoparser by @sv1zx ❤️
                        """)
            except:
                pass
        await update.message.reply_text(f"Парсинг завершен, найдено {found} объявлений")
        Logger.info('PARSE', f'Parsed {found} cards')
        STARTED_PARS.remove(update.effective_user.id)
    except Exception as e:
        Logger.error("PARSE", e)
        await Logger.save()
    finally:
        Logger.info('PARSE', "Parse closed")
        await Logger.save()
        await update.message.reply_document(Logger.get_file(), caption="Лог парсера")

@check_admin
async def broadcast(update: Update, context):
    try:
        for user in ALLOWED_USER_ID:
            await update.get_bot().send_message(user, context.args[0])
    except Exception:
        await update.message.reply_text("Введите текст обьявления")

@check_user_access
async def parsec(update: Update, context):
    asyncio.create_task(parse(update, context, ParseLogger(update.effective_user.id)))

@check_user_access
async def preset(update: Update, context):
    data = dict()
    async with aiofiles.open('presets.json', 'r', encoding='utf-8') as f:
        try:
            data = json.loads(await f.read())
        except:
            data.update({"presets": []})
    try:
        async with aiofiles.open('presets.json', 'w+', encoding='utf-8') as f:
            if not any(d for d in data['presets'] if d["id"] == update.effective_user.id and d["name"] == context.args[0]):
                data['presets'].append({"id": update.effective_user.id, "name": context.args[0],
                                        "preset": f"{context.args[1]} {context.args[2]} {context.args[3]} {context.args[4]} {context.args[5]} {context.args[6]} {context.args[7]}"})
                await f.write(json.dumps(data))
            else:
                await update.message.reply_text(f"Такой пресет уже существует\n\n❤️ olx.kz neoparser by @sv1zx ❤️")
                return
    except:
        await update.message.reply_text("Введите параметры")
    await update.message.reply_text(f"Успешно добавлен пресет {context.args[0]}\n\n❤️ olx.kz neoparser by @sv1zx ❤️")

@check_user_access
async def stop(update: Update, context):
    global STARTED_PARS
    if update.effective_user.id in STARTED_PARS:
        STARTED_PARS.remove(update.effective_user.id)
    else:
        await update.message.reply_text("Вы не запускали парсер")

if __name__ == "__main__":
    load_dotenv()
    with open("allowed.txt", "r", ) as file:
        for line in file:
            print(f"{line} Allowed to use")
            ALLOWED_USER_ID.append(int(line))
    app = ApplicationBuilder().token(os.getenv("TOKEN")).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("parse", parsec))
    app.add_handler(CommandHandler("preset", preset))
    app.add_handler(CommandHandler("stop", stop))
    app.add_handler(CommandHandler("broadcast", broadcast))
    app.run_polling()
