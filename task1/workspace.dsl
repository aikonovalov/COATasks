workspace "Маркетплейс" "c4 диаграмма маркетплейса" {

    !identifiers hierarchical

    model {
        user = person "Пользователь"
        customer = person "Продавец"
        
        marketplaceSystem = softwareSystem "Маркетплейс" "Маркетплейс" {
            webApp = container "Web" "Service"
            mobileApp = container "Mobile" "Service"
            
            apiGateway = container "API Gateway" "Service"

            recsysService = container "Рекомендательная система" "Service" "Python"
            catalogService = container "Каталог товаров" "Service" "C++"
            orderService = container "Сервис заказов" "Service" "GO"
            cartService = container "Корзина" "Service" "GO"
            paymentService = container "Оплата" "Service" "C++"
            notificationService = container "Уведомления" "Service" "C++"
            
            catalogDB = container "БД каталога" "Database" "PostgreSQL"
            orderDB = container "БД заказов" "Database" "PostgreSQL"
            cartDB = container "БД корзин" "Database" "PostgreSQL"
            paymentDB = container "БД транзакций" "Database" "PostgreSQL"
            recsysDB = container "БД рексиса" "Database" "PostgreSQL" 

            messageQueue = container "Брокер сообщений" "RabbitMQ"
        }
        
        user -> marketplaceSystem.webApp "Просматривает и покупает товары" "HTTPS"
        user -> marketplaceSystem.mobileApp "Просматривает и покупает товары" "HTTPS"

        customer -> marketplaceSystem.webApp "Выкладывает свои товары" "HTTPS"
        customer -> marketplaceSystem.mobileApp "Выкладывает свои товары" "HTTPS"
        
        marketplaceSystem.webApp -> marketplaceSystem.apiGateway "Отправляет запросы" "REST"
        marketplaceSystem.mobileApp -> marketplaceSystem.apiGateway "Отправляет запросы" "REST"

        marketplaceSystem.apiGateway -> marketplaceSystem.recsysService "Запрашивает рекомендации" "REST"
        marketplaceSystem.recsysService -> marketplaceSystem.catalogService "Запрашивает товары для биддинга" "REST"

        marketplaceSystem.apiGateway -> marketplaceSystem.catalogService "Результаты поиска товара" "REST"
        marketplaceSystem.apiGateway -> marketplaceSystem.orderService "Просмотр текущих статусов заказов" "REST"
        marketplaceSystem.apiGateway -> marketplaceSystem.cartService "Управление корзиной" "REST"
        marketplaceSystem.apiGateway -> marketplaceSystem.paymentService "Инициирует оплату" "REST"

        marketplaceSystem.catalogService -> marketplaceSystem.catalogDB "Получает данные" "SQL"
        marketplaceSystem.orderService -> marketplaceSystem.orderDB "Получает заказы" "SQL"
         marketplaceSystem.paymentService -> marketplaceSystem.paymentDB "Сохраняет транзакции" "SQL"
        marketplaceSystem.recsysService -> marketplaceSystem.recsysDB "Хранит историю и модели" "SQL"
        marketplaceSystem.cartService -> marketplaceSystem.cartDB "Управляет корзинами" "Redis Protocol"

        marketplaceSystem.orderService -> marketplaceSystem.messageQueue "Событие OrderCreated" "AMQP"

        marketplaceSystem.messageQueue -> marketplaceSystem.notificationService "Слушает события заказов" "AMQP"
        marketplaceSystem.messageQueue -> marketplaceSystem.paymentService "Слушает события заказов" "AMQP"
        
        marketplaceSystem.paymentService -> marketplaceSystem.messageQueue "Публикует PaymentCompleted" "AMQP"
        marketplaceSystem.messageQueue -> marketplaceSystem.catalogService "Обновляет остатки товаров" "AMQP"







    }

    views {
        systemContext marketplaceSystem "SystemContext" {
            include *
            autolayout lr
        }

        container marketplaceSystem "Containers" {
            include *
            autolayout tb
        }
    }

}